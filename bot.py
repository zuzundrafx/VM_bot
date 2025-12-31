import telebot
from telebot import types
import openpyxl
import datetime
import time
import logging
import requests
from urllib.parse import urlencode
import os
from dotenv import load_dotenv
from flask import Flask, request, jsonify
import traceback
import threading

# Загрузка переменных окружения
load_dotenv()

# Настройка расширенного логирования
logging.basicConfig(
    level=logging.DEBUG,  # Измените на INFO в продакшене
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot_debug.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Безопасное получение токена
TOKEN = os.getenv('BOT_TOKEN', '7084552505:AAECx4YcUNDJV9SV-Dd4VEddpjyBnR_IBiA')
if not TOKEN:
    logger.error("❌ BOT_TOKEN не найден в переменных окружения!")
    logger.error("Добавьте BOT_TOKEN в переменные окружения или .env файл")
    raise ValueError("BOT_TOKEN не установлен")

logger.info(f"✅ Бот инициализирован с токеном: {TOKEN[:10]}...")

bot = telebot.TeleBot(TOKEN, parse_mode='HTML')
app = Flask(__name__)

# Глобальные переменные для кеширования
excel_cache = {
    'file_path': None,
    'timestamp': None,
    'data': None,
    'lock': threading.Lock()
}

CACHE_TIMEOUT = 300  # 5 минут кеширования

class ExcelProcessor:
    """Класс для обработки Excel файлов"""
    
    def __init__(self):
        self.columns_map = {}
        
    def find_user_row(self, sheet, user_id):
        """Найти строку пользователя по ID"""
        logger.debug(f"Поиск пользователя с ID: {user_id}")
        
        for row in sheet.iter_rows(min_row=2, max_row=100, min_col=1, max_col=10):
            cell_value = row[0].value
            if cell_value is not None:
                try:
                    if str(cell_value).strip() == str(user_id).strip():
                        logger.info(f"✅ Пользователь найден в строке {row[0].row}")
                        return row[0].row
                except Exception as e:
                    logger.debug(f"Ошибка сравнения ID: {e}")
                    
        logger.warning(f"❌ Пользователь с ID {user_id} не найден")
        return None
    
    def map_columns(self, sheet):
        """Создать карту колонок по заголовкам"""
        logger.debug("Создание карты колонок...")
        self.columns_map = {}
        
        # Ищем заголовки в первой строке
        header_row = 1
        for col in range(1, sheet.max_column + 1):
            header = sheet.cell(row=header_row, column=col).value
            if header:
                self.columns_map[header] = col
                logger.debug(f"  {header} -> колонка {col}")
        
        logger.info(f"✅ Найдено {len(self.columns_map)} колонок")
        return self.columns_map
    
    def get_cell_value(self, sheet, row, column_name, default="Не указано"):
        """Безопасное получение значения ячейки"""
        if not self.columns_map:
            self.map_columns(sheet)
            
        col_idx = self.columns_map.get(column_name)
        if not col_idx or row is None:
            return default
            
        value = sheet.cell(row=row, column=col_idx).value
        return value if value is not None else default
    
    def format_currency(self, value):
        """Форматирование денежных значений"""
        if value is None:
            return "0"
        try:
            if isinstance(value, (int, float)):
                return f"{value:,.0f}".replace(",", " ")
            return str(value)
        except:
            return str(value)

# Инициализация процессора Excel
excel_processor = ExcelProcessor()

def download_excel_file(force_refresh=False):
    """Скачивание и кеширование Excel файла"""
    with excel_cache['lock']:
        current_time = time.time()
        
        # Проверяем кеш
        if (not force_refresh and 
            excel_cache['file_path'] and 
            excel_cache['timestamp'] and 
            (current_time - excel_cache['timestamp']) < CACHE_TIMEOUT):
            logger.info("✅ Используем кешированный файл")
            return excel_cache['file_path']
        
        try:
            logger.info("⬇️ Начинаем загрузку Excel файла...")
            
            # Публичная ссылка на Яндекс.Диск
            base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
            public_key = 'https://disk.yandex.ru/i/gFvPIdO1gBanpw'
            
            # Получаем прямую ссылку для скачивания
            final_url = base_url + urlencode({'public_key': public_key})
            logger.debug(f"Запрос ссылки: {final_url}")
            
            response = requests.get(final_url, timeout=15)
            response.raise_for_status()
            
            download_data = response.json()
            download_url = download_data.get('href')
            
            if not download_url:
                logger.error("❌ Не удалось получить ссылку для скачивания")
                return None
            
            logger.debug(f"Прямая ссылка: {download_url[:100]}...")
            
            # Скачиваем файл
            download_response = requests.get(download_url, timeout=30)
            download_response.raise_for_status()
            
            # Сохраняем файл
            file_path = 'actual_tabel.xlsx'
            with open(file_path, 'wb') as f:
                f.write(download_response.content)
            
            # Проверяем, что файл валидный
            try:
                test_wb = openpyxl.load_workbook(file_path, data_only=True, read_only=True)
                test_wb.close()
            except Exception as e:
                logger.error(f"❌ Загруженный файл не является валидным Excel: {e}")
                return None
            
            # Обновляем кеш
            excel_cache['file_path'] = file_path
            excel_cache['timestamp'] = current_time
            excel_cache['data'] = None
            
            file_size = os.path.getsize(file_path)
            logger.info(f"✅ Файл успешно скачан ({file_size:,} байт)")
            
            return file_path
            
        except requests.exceptions.Timeout:
            logger.error("❌ Таймаут при загрузке файла")
            return None
        except requests.exceptions.ConnectionError:
            logger.error("❌ Ошибка соединения при загрузке файла")
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка скачивания файла: {str(e)}")
            logger.error(traceback.format_exc())
            return None

def load_excel_data(file_path):
    """Загрузка данных из Excel файла"""
    try:
        logger.debug(f"Загрузка Excel файла: {file_path}")
        
        # Используем read_only для больших файлов
        wb = openpyxl.load_workbook(
            file_path, 
            data_only=True, 
            read_only=True
        )
        
        sheet = wb.active
        
        # Получаем дату из файла (предполагаем, что она в ячейке B1)
        file_date = sheet.cell(row=1, column=2).value
        logger.info(f"📅 Дата в файле: {file_date}")
        
        return {
            'wb': wb,
            'sheet': sheet,
            'date': file_date
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки Excel: {str(e)}")
        logger.error(traceback.format_exc())
        return None

@bot.message_handler(commands=['start'])
def handle_start(message):
    """Обработчик команды /start"""
    try:
        logger.info(f"🆕 Команда /start от пользователя {message.from_user.id}")
        
        # Создаем клавиатуру
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton("📊 Общая информация")
        btn2 = types.KeyboardButton("💰 Табель")
        btn3 = types.KeyboardButton("🔄 Обновить данные")
        btn4 = types.KeyboardButton("ℹ️ Помощь")
        markup.add(btn1, btn2, btn3, btn4)
        
        # Отправляем приветственное сообщение
        welcome_text = (
            f"👋 Здравствуйте, <b>{message.from_user.first_name}</b>!\n\n"
            f"<i>Бот для просмотра информации из табеля</i>\n"
            f"▫️ Версия: <b>1.2</b>\n"
            f"▫️ Ваш ID: <code>{message.from_user.id}</code>\n\n"
            f"Выберите нужный раздел:"
        )
        
        bot.send_message(
            message.chat.id, 
            welcome_text,
            parse_mode='HTML',
            reply_markup=markup
        )
        
    except Exception as e:
        logger.error(f"❌ Ошибка в handle_start: {str(e)}")
        bot.send_message(message.chat.id, "❌ Произошла ошибка. Попробуйте позже.")

@bot.message_handler(func=lambda message: message.text == "📊 Общая информация")
def handle_general_info(message):
    """Обработчик общей информации"""
    try:
        logger.info(f"📋 Запрос общей информации от {message.from_user.id}")
        bot.send_message(message.chat.id, "⏳ Собираю информацию о вас...")
        
        # Скачиваем файл
        file_path = download_excel_file()
        if not file_path:
            bot.send_message(
                message.chat.id,
                "❌ Не удалось загрузить данные. Возможные причины:\n"
                "• Нет доступа к файлу\n"
                "• Проблемы с интернетом\n"
                "• Файл удален или перемещен"
            )
            return
        
        # Загружаем данные
        excel_data = load_excel_data(file_path)
        if not excel_data:
            bot.send_message(message.chat.id, "❌ Ошибка чтения файла данных")
            return
        
        sheet = excel_data['sheet']
        user_id = message.from_user.id
        
        # Ищем пользователя
        user_row = excel_processor.find_user_row(sheet, user_id)
        if not user_row:
            bot.send_message(
                message.chat.id,
                "❌ Ваш ID не найден в базе данных.\n"
                "Убедитесь, что вы правильно зарегистрированы."
            )
            excel_data['wb'].close()
            return
        
        # Создаем карту колонок
        excel_processor.map_columns(sheet)
        
        # Получаем данные
        full_name = excel_processor.get_cell_value(sheet, user_row, "ФИО")
        position = excel_processor.get_cell_value(sheet, user_row, "Должность")
        hire_date = excel_processor.get_cell_value(sheet, user_row, "Дата приема")
        experience_coef = excel_processor.get_cell_value(sheet, user_row, "Текущий стаж.коэфф")
        bonus_coef = excel_processor.get_cell_value(sheet, user_row, "За опыт!")
        
        # Форматируем ответ
        response_text = (
            f"<b>👤 Личная информация</b>\n"
            f"▫️ <b>ФИО:</b> {full_name}\n"
            f"▫️ <b>Должность:</b> {position}\n"
            f"▫️ <b>Дата приема:</b> {hire_date}\n"
            f"▫️ <b>Коэффициент стажа:</b> {experience_coef}%\n"
            f"▫️ <b>Коэффициент опыта:</b> {bonus_coef}%\n\n"
            f"<i>Данные актуальны на: {excel_data.get('date', 'неизвестно')}</i>"
        )
        
        bot.send_message(message.chat.id, response_text, parse_mode='HTML')
        excel_data['wb'].close()
        
    except Exception as e:
        logger.error(f"❌ Ошибка в handle_general_info: {str(e)}")
        logger.error(traceback.format_exc())
        bot.send_message(message.chat.id, "❌ Произошла ошибка при обработке запроса.")

@bot.message_handler(func=lambda message: message.text == "💰 Табель")
def handle_tabel(message):
    """Обработчик запроса табеля"""
    try:
        logger.info(f"💰 Запрос табеля от {message.from_user.id}")
        bot.send_message(message.chat.id, "⏳ Формирую отчет по зарплате...")
        
        # Скачиваем файл
        file_path = download_excel_file()
        if not file_path:
            bot.send_message(message.chat.id, "❌ Не удалось загрузить данные")
            return
        
        # Загружаем данные
        excel_data = load_excel_data(file_path)
        if not excel_data:
            bot.send_message(message.chat.id, "❌ Ошибка чтения файла данных")
            return
        
        sheet = excel_data['sheet']
        user_id = message.from_user.id
        
        # Ищем пользователя
        user_row = excel_processor.find_user_row(sheet, user_id)
        if not user_row:
            bot.send_message(
                message.chat.id,
                "❌ Ваш ID не найден в базе данных."
            )
            excel_data['wb'].close()
            return
        
        # Создаем карту колонок
        excel_processor.map_columns(sheet)
        
        # Получаем данные
        full_name = excel_processor.get_cell_value(sheet, user_row, "ФИО")
        
        # Основные данные
        hours = excel_processor.get_cell_value(sheet, user_row, "Итого часов")
        hourly_pay = excel_processor.get_cell_value(sheet, user_row, "ЗП (почасовка) без учета компенсаций")
        bonus = excel_processor.get_cell_value(sheet, user_row, "Премия")
        experience_pay = excel_processor.get_cell_value(sheet, user_row, "ЗП за Опыт")
        seniority_pay = excel_processor.get_cell_value(sheet, user_row, "ЗП за Стаж")
        housing = excel_processor.get_cell_value(sheet, user_row, "Квартиры")
        total_salary = excel_processor.get_cell_value(sheet, user_row, "ЗП (почасовка + премии + стаж + опыт + квартиры)")
        compensations = excel_processor.get_cell_value(sheet, user_row, "Компенсации")
        
        # Форматируем числа
        hours = excel_processor.format_currency(hours)
        hourly_pay = excel_processor.format_currency(hourly_pay)
        bonus = excel_processor.format_currency(bonus)
        experience_pay = excel_processor.format_currency(experience_pay)
        seniority_pay = excel_processor.format_currency(seniority_pay)
        housing = excel_processor.format_currency(housing)
        total_salary = excel_processor.format_currency(total_salary)
        compensations = excel_processor.format_currency(compensations)
        
        # Форматируем ответ
        response_text = (
            f"<b>💰 Отчет по зарплате</b>\n"
            f"▫️ <b>ФИО:</b> {full_name}\n"
            f"▫️ <b>Период:</b> {excel_data.get('date', 'неизвестно')}\n\n"
            
            f"<b>📊 Основные показатели:</b>\n"
            f"• Отработано часов: <b>{hours}</b>\n"
            f"• Почасовая оплата: <b>{hourly_pay} ₽</b>\n"
            f"• Премия: <b>{bonus} ₽</b>\n"
            f"• За опыт: <b>{experience_pay} ₽</b>\n"
            f"• За стаж: <b>{seniority_pay} ₽</b>\n"
            f"• Квартирные: <b>{housing} ₽</b>\n\n"
            
            f"<b>💵 Итоги:</b>\n"
            f"• <b>Общая зарплата: {total_salary} ₽</b>\n"
            f"• Компенсации: {compensations} ₽\n\n"
            
            f"<i>Вся информация предоставлена для ознакомления.</i>"
        )
        
        bot.send_message(message.chat.id, response_text, parse_mode='HTML')
        excel_data['wb'].close()
        
    except Exception as e:
        logger.error(f"❌ Ошибка в handle_tabel: {str(e)}")
        logger.error(traceback.format_exc())
        bot.send_message(message.chat.id, "❌ Произошла ошибка при формировании отчета.")

@bot.message_handler(func=lambda message: message.text == "🔄 Обновить данные")
def handle_refresh(message):
    """Принудительное обновление данных"""
    try:
        logger.info(f"🔄 Принудительное обновление от {message.from_user.id}")
        bot.send_message(message.chat.id, "🔄 Принудительно обновляю данные...")
        
        # Очищаем кеш и скачиваем заново
        with excel_cache['lock']:
            excel_cache['file_path'] = None
            excel_cache['timestamp'] = None
            excel_cache['data'] = None
        
        file_path = download_excel_file(force_refresh=True)
        
        if file_path:
            bot.send_message(message.chat.id, "✅ Данные успешно обновлены!")
        else:
            bot.send_message(message.chat.id, "❌ Не удалось обновить данные")
            
    except Exception as e:
        logger.error(f"❌ Ошибка в handle_refresh: {str(e)}")
        bot.send_message(message.chat.id, "❌ Ошибка при обновлении данных")

@bot.message_handler(func=lambda message: message.text == "ℹ️ Помощь")
def handle_help(message):
    """Обработчик помощи"""
    help_text = (
        "<b>ℹ️ Справка по боту</b>\n\n"
        "<b>Доступные команды:</b>\n"
        "• <b>📊 Общая информация</b> - ваши личные данные\n"
        "• <b>💰 Табель</b> - отчет по зарплате\n"
        "• <b>🔄 Обновить данные</b> - обновить информацию из файла\n"
        "• <b>/start</b> - перезапустить бота\n\n"
        
        "<b>Частые проблемы:</b>\n"
        "• Если данные не обновляются - нажмите 'Обновить данные'\n"
        "• Если не находите себя - проверьте правильность ID\n"
        "• Файл обновляется вручную администратором\n\n"
        
        "<b>Техническая поддержка:</b>\n"
        "При проблемах свяжитесь с администратором."
    )
    
    bot.send_message(message.chat.id, help_text, parse_mode='HTML')

@bot.message_handler(func=lambda message: True)
def handle_unknown(message):
    """Обработчик неизвестных команд"""
    logger.warning(f"❓ Неизвестная команда от {message.from_user.id}: {message.text}")
    
    response_text = (
        "🤔 Не понимаю вашу команду.\n\n"
        "Используйте кнопки меню или команды:\n"
        "• /start - перезапустить бота\n"
        "• 'Помощь' - справка по боту"
    )
    
    bot.send_message(message.chat.id, response_text)

# Webhook обработчики для Render
@app.route('/')
def index():
    return jsonify({
        'status': 'running',
        'service': 'Telegram Bot',
        'timestamp': datetime.datetime.now().isoformat()
    })

@app.route('/webhook', methods=['POST'])
def webhook():
    """Обработчик вебхука от Telegram"""
    try:
        if request.headers.get('content-type') == 'application/json':
            json_string = request.get_data().decode('utf-8')
            update = telebot.types.Update.de_json(json_string)
            bot.process_new_updates([update])
            logger.debug("✅ Webhook успешно обработан")
            return ''
        else:
            logger.warning("❌ Неверный content-type в webhook")
            return 'Invalid content type', 400
    except Exception as e:
        logger.error(f"❌ Ошибка в webhook: {str(e)}")
        logger.error(traceback.format_exc())
        return 'Internal server error', 500

@app.route('/health')
def health_check():
    """Проверка здоровья приложения"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.datetime.now().isoformat(),
        'cache_age': time.time() - excel_cache.get('timestamp', 0) if excel_cache.get('timestamp') else None
    })

def setup_webhook():
    """Настройка вебхука для Render"""
    try:
        hostname = os.environ.get('RENDER_EXTERNAL_HOSTNAME')
        
        if not hostname:
            logger.warning("⚠️ RENDER_EXTERNAL_HOSTNAME не найден, запускаем в polling режиме")
            return False
        
        webhook_url = f"https://{hostname}/webhook"
        logger.info(f"🌐 Настройка вебхука на: {webhook_url}")
        
        # Удаляем старый вебхук
        bot.remove_webhook()
        time.sleep(1)
        
        # Устанавливаем новый
        bot.set_webhook(url=webhook_url)
        
        # Проверяем установку
        webhook_info = bot.get_webhook_info()
        logger.info(f"ℹ️ Информация о вебхуке: {webhook_info}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка настройки вебхука: {str(e)}")
        return False

def run_polling():
    """Запуск бота в polling режиме (для локальной разработки)"""
    logger.info("🚀 Запуск бота в polling режиме...")
    try:
        bot.remove_webhook()
        bot.polling(none_stop=True, interval=2, timeout=30)
    except Exception as e:
        logger.error(f"❌ Ошибка polling: {str(e)}")
        time.sleep(5)
        run_polling()  # Перезапуск при ошибке

if __name__ == '__main__':
    logger.info("=" * 50)
    logger.info("🚀 Запуск Telegram бота")
    logger.info("=" * 50)
    
    # Инициализация при первом запуске
    download_excel_file()
    
    # Определяем режим запуска
    if os.environ.get('RENDER'):
        # Режим вебхука для Render
        logger.info("🌍 Запуск в режиме вебхука (Render)")
        
        if setup_webhook():
            port = int(os.environ.get('PORT', 5000))
            logger.info(f"🌐 Запуск Flask на порту {port}")
            app.run(host='0.0.0.0', port=port, debug=False)
        else:
            logger.error("❌ Не удалось настроить вебхук, падаем")
    else:
        # Режим polling для локального запуска
        logger.info("💻 Запуск в polling режиме (локально)")
        run_polling()
