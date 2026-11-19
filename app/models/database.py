"""
Database layer for FindHome Chile
Supports MySQL (MariaDB) with graceful SQLite fallback
"""
import os
import json
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

DB_TYPE_ENV = os.getenv("DB_TYPE", "").strip().lower()
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "lookingforhome")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "LookingForHome2026!")
MYSQL_DB = os.getenv("MYSQL_DB", "findhome_chile")

SQLITE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "findhome.db")
_ACTIVE_ENGINE = None

INITIAL_PROPERTIES = [
    {
        "title": "Gran Departamento de 4 Dormitorios y Terraza en Providencia",
        "subdivision": "Región Metropolitana (Santiago)",
        "subdivision_code": "RM",
        "city": "Santiago - Providencia",
        "address": "Av. Pedro de Valdivia # 1420, Providencia",
        "neighborhood": "Pedro de Valdivia / Inés de Suárez",
        "latitude": -33.4385,
        "longitude": -70.6124,
        "total_rent": 1200000,
        "bedrooms": 4,
        "bathrooms": 3,
        "surface": 140,
        "portal": "Portal Inmobiliario",
        "url": "https://www.portalinmobiliario.com/arriendo/departamento/providencia/prop1",
        "desc": "Espacioso departamento en sector residencial consolidado, a pasos del Metro Inés de Suárez, calefacción central y bodega.",
        "contact_name": "Providencia Co-Living",
        "contact_phone": "+56 9 5555 0182",
        "contact_agency": "Santiago Propiedades"
    },
    {
        "title": "Exclusivo Departamento de 3 Habitaciones en Las Condes / El Golf",
        "subdivision": "Región Metropolitana (Santiago)",
        "subdivision_code": "RM",
        "city": "Santiago - Las Condes",
        "address": "Av. Apoquindo # 3600, Las Condes",
        "neighborhood": "El Golf / Escuela Militar",
        "latitude": -33.4182,
        "longitude": -70.5925,
        "total_rent": 1500000,
        "bedrooms": 3,
        "bathrooms": 3,
        "surface": 145,
        "portal": "TocToc Direct",
        "url": "https://www.toctoc.com/propiedades/arriendo/las-condes/prop2",
        "desc": "Sector financiero y gastronómico de Santiago. Piso alto con vista a la Cordillera de los Andes, piscina y seguridad 24/7.",
        "contact_name": "El Golf Executives",
        "contact_phone": "+56 9 5555 0199",
        "contact_agency": "Andes Real Estate"
    },
    {
        "title": "Luminoso Departamento de 3 Dormitorios en Barrio Italia / Ñuñoa",
        "subdivision": "Región Metropolitana (Santiago)",
        "subdivision_code": "RM",
        "city": "Santiago - Ñuñoa",
        "address": "Av. Santa Isabel # 850, Ñuñoa",
        "neighborhood": "Barrio Italia",
        "latitude": -33.4498,
        "longitude": -70.6265,
        "total_rent": 950000,
        "bedrooms": 3,
        "bathrooms": 2,
        "surface": 115,
        "portal": "Portal Inmobiliario",
        "url": "https://www.portalinmobiliario.com/arriendo/nunoa-barrio-italia/prop3",
        "desc": "Foco de diseño, cafeterías y cultura. Excelente distribución, cocina americana integrada y amplia logia independiente.",
        "contact_name": "Barrio Italia Co-Living",
        "contact_phone": "+56 9 5555 0147",
        "contact_agency": "Ñuñoa Urbano"
    },
    {
        "title": "Casona Tradicional de 4 Dormitorios en Cerro Alegre con Vista a la Bahía",
        "subdivision": "Valparaíso",
        "subdivision_code": "VAL",
        "city": "Valparaíso",
        "address": "Paseo Yugoslavo # 120, Cerro Alegre",
        "neighborhood": "Cerro Alegre",
        "latitude": -33.0416,
        "longitude": -71.6288,
        "total_rent": 850000,
        "bedrooms": 4,
        "bathrooms": 2,
        "surface": 160,
        "portal": "Yapo.cl Viviendas",
        "url": "https://www.yapo.cl/valparaiso/cerro-alegre/prop4",
        "desc": "Arquitectura patrimonial porteña con pisos de madera noble, ventanales al mar y terraza para atardeceres frente al puerto.",
        "contact_name": "Valpo Bohemia Living",
        "contact_phone": "+56 9 5555 0134",
        "contact_agency": "Costa Central Propiedades"
    },
    {
        "title": "Moderno Departamento de 3 Dormitorios frente a Laguna Grande",
        "subdivision": "Biobío (Concepción)",
        "subdivision_code": "BIO",
        "city": "Concepción - San Pedro de la Paz",
        "address": "Av. Los Canelos # 780, San Pedro de la Paz",
        "neighborhood": "Laguna Grande",
        "latitude": -36.8452,
        "longitude": -73.0885,
        "total_rent": 750000,
        "bedrooms": 3,
        "bathrooms": 2,
        "surface": 120,
        "portal": "Portal Inmobiliario",
        "url": "https://www.portalinmobiliario.com/arriendo/concepcion/prop5",
        "desc": "Entorno natural privilegiado rodeado de vegetación y deportes náuticos. Ventanales termopanel, quincho y gimnasio.",
        "contact_name": "Biobío Minicomunidades",
        "contact_phone": "+56 9 5555 0168",
        "contact_agency": "Sur Inmobiliaria"
    }
]
SUBDIVISIONS = [
    [
        "Región Metropolitana (Santiago)",
        "RM"
    ],
    [
        "Valparaíso",
        "VAL"
    ],
    [
        "Biobío (Concepción)",
        "BIO"
    ],
    [
        "Coquimbo (La Serena)",
        "COQ"
    ],
    [
        "Antofagasta",
        "ANT"
    ],
    [
        "Araucanía (Temuco)",
        "ARA"
    ],
    [
        "Los Lagos (Puerto Montt)",
        "LL"
    ],
    [
        "Maule (Talca)",
        "MAU"
    ],
    [
        "O'Higgins (Rancagua)",
        "OHI"
    ]
]

def detect_engine() -> str:
    global _ACTIVE_ENGINE
    if _ACTIVE_ENGINE is not None:
        return _ACTIVE_ENGINE
    if DB_TYPE_ENV == "sqlite":
        _ACTIVE_ENGINE = "sqlite"
        return _ACTIVE_ENGINE
    try:
        import pymysql
        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
            password=MYSQL_PASSWORD, database=MYSQL_DB, connect_timeout=3
        )
        conn.close()
        _ACTIVE_ENGINE = "mysql"
        return _ACTIVE_ENGINE
    except Exception:
        _ACTIVE_ENGINE = "sqlite"
        return _ACTIVE_ENGINE

def get_raw_connection():
    engine = detect_engine()
    if engine == "mysql":
        import pymysql
        import pymysql.cursors
        return pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
            password=MYSQL_PASSWORD, database=MYSQL_DB, charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor, autocommit=False
        )
    else:
        import sqlite3
        conn = sqlite3.connect(SQLITE_PATH)
        conn.row_factory = sqlite3.Row
        return conn

def init_db():
    engine = detect_engine()
    conn = get_raw_connection()
    cursor = conn.cursor()

    if engine == "mysql":
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS properties (
            id VARCHAR(64) PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            description TEXT,
            city VARCHAR(100) NOT NULL,
            subdivision VARCHAR(100) NOT NULL,
            subdivision_code VARCHAR(10),
            address VARCHAR(255),
            neighborhood VARCHAR(100),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            total_rent DOUBLE PRECISION NOT NULL,
            ideal_roommates INT DEFAULT 4,
            bedrooms INT NOT NULL,
            bathrooms INT NOT NULL,
            surface INT NOT NULL,
            has_elevator TINYINT(1) DEFAULT 1,
            is_exterior TINYINT(1) DEFAULT 1,
            is_furnished TINYINT(1) DEFAULT 1,
            has_terrace TINYINT(1) DEFAULT 0,
            has_air_con TINYINT(1) DEFAULT 0,
            source_portal VARCHAR(100) DEFAULT 'Direct Portal',
            source_url VARCHAR(500),
            property_type VARCHAR(50) DEFAULT 'apartment',
            images JSON,
            contact_name VARCHAR(150),
            contact_phone VARCHAR(50),
            contact_agency VARCHAR(150),
            status VARCHAR(30) DEFAULT 'available',
            created_at VARCHAR(50)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS access_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ip VARCHAR(64) NOT NULL,
            country VARCHAR(100),
            country_code VARCHAR(10),
            region VARCHAR(100),
            city VARCHAR(100),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            path VARCHAR(255),
            method VARCHAR(10),
            status_code INT,
            user_agent TEXT,
            device_type VARCHAR(30),
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS whatsapp_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sender_phone VARCHAR(50),
            sender_name VARCHAR(100),
            message_body TEXT,
            response_body TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        conn.commit()

        # Check if properties exist
        cursor.execute("SELECT COUNT(*) AS cnt FROM properties")
        row = cursor.fetchone()
        cnt = row["cnt"] if isinstance(row, dict) else row[0]
        if cnt == 0:
            seed_data(cursor, engine)
            conn.commit()

    else:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS properties (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT,
            city TEXT NOT NULL,
            subdivision TEXT NOT NULL,
            subdivision_code TEXT,
            address TEXT,
            neighborhood TEXT,
            latitude REAL,
            longitude REAL,
            total_rent REAL NOT NULL,
            ideal_roommates INTEGER DEFAULT 4,
            bedrooms INTEGER NOT NULL,
            bathrooms INTEGER NOT NULL,
            surface INTEGER NOT NULL,
            has_elevator INTEGER DEFAULT 1,
            is_exterior INTEGER DEFAULT 1,
            is_furnished INTEGER DEFAULT 1,
            has_terrace INTEGER DEFAULT 0,
            has_air_con INTEGER DEFAULT 0,
            source_portal TEXT,
            source_url TEXT,
            property_type TEXT,
            images TEXT,
            contact_name TEXT,
            contact_phone TEXT,
            contact_agency TEXT,
            status TEXT DEFAULT 'available',
            created_at TEXT
        );
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS access_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ip TEXT NOT NULL,
            country TEXT,
            country_code TEXT,
            region TEXT,
            city TEXT,
            latitude REAL,
            longitude REAL,
            path TEXT,
            method TEXT,
            status_code INTEGER,
            user_agent TEXT,
            device_type TEXT,
            timestamp TEXT
        );
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS whatsapp_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_phone TEXT,
            sender_name TEXT,
            message_body TEXT,
            response_body TEXT,
            timestamp TEXT
        );
        """)
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM properties")
        if cursor.fetchone()[0] == 0:
            seed_data(cursor, engine)
            conn.commit()

    conn.close()

def seed_data(cursor, engine):
    img_list = [
        "static/images/prop-mad-01-1.jpg", "static/images/prop-mad-01-2.jpg",
        "static/images/prop-bcn-01-1.jpg", "static/images/prop-par-01-1.jpg",
        "static/images/prop-ber-01-1.jpg", "static/images/prop-lis-01-1.jpg"
    ]
    for idx, p in enumerate(INITIAL_PROPERTIES):
        pid = f"chile-prop-{idx+1:02d}"
        imgs = json.dumps([img_list[idx % len(img_list)]])
        now = datetime.now(timezone.utc).isoformat()
        if engine == "mysql":
            cursor.execute("""
            INSERT INTO properties (
                id, title, description, city, subdivision, subdivision_code, address, neighborhood,
                latitude, longitude, total_rent, ideal_roommates, bedrooms, bathrooms, surface,
                has_elevator, is_exterior, is_furnished, has_terrace, has_air_con,
                source_portal, source_url, property_type, images, contact_name, contact_phone, contact_agency, status, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                pid, p["title"], p["desc"], p["city"], p["subdivision"], p.get("subdivision_code", ""),
                p["address"], p["neighborhood"], p["latitude"], p["longitude"], p["total_rent"],
                p.get("ideal_roommates", p["bedrooms"]), p["bedrooms"], p["bathrooms"], p["surface"],
                1, 1, 1, 1, 1, p["portal"], p["url"], "apartment", imgs,
                p["contact_name"], p["contact_phone"], p["contact_agency"], "available", now
            ))
        else:
            cursor.execute("""
            INSERT INTO properties (
                id, title, description, city, subdivision, subdivision_code, address, neighborhood,
                latitude, longitude, total_rent, ideal_roommates, bedrooms, bathrooms, surface,
                has_elevator, is_exterior, is_furnished, has_terrace, has_air_con,
                source_portal, source_url, property_type, images, contact_name, contact_phone, contact_agency, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pid, p["title"], p["desc"], p["city"], p["subdivision"], p.get("subdivision_code", ""),
                p["address"], p["neighborhood"], p["latitude"], p["longitude"], p["total_rent"],
                p.get("ideal_roommates", p["bedrooms"]), p["bedrooms"], p["bathrooms"], p["surface"],
                1, 1, 1, 1, 1, p["portal"], p["url"], "apartment", imgs,
                p["contact_name"], p["contact_phone"], p["contact_agency"], "available", now
            ))

def get_properties(city=None, subdivision=None, portal=None, min_bedrooms=None, max_total_rent=None) -> List[Dict[str, Any]]:
    conn = get_raw_connection()
    cursor = conn.cursor()
    query = "SELECT * FROM properties WHERE 1=1"
    params = []

    if city:
        query += " AND LOWER(city) LIKE %s" if detect_engine() == "mysql" else " AND LOWER(city) LIKE ?"
        params.append(f"%{city.lower()}%")
    if subdivision:
        query += " AND (LOWER(subdivision) LIKE %s OR LOWER(subdivision_code) LIKE %s)" if detect_engine() == "mysql" else " AND (LOWER(subdivision) LIKE ? OR LOWER(subdivision_code) LIKE ?)"
        params.extend([f"%{subdivision.lower()}%", f"%{subdivision.lower()}%"])
    if portal:
        query += " AND LOWER(source_portal) LIKE %s" if detect_engine() == "mysql" else " AND LOWER(source_portal) LIKE ?"
        params.append(f"%{portal.lower()}%")
    if min_bedrooms:
        query += " AND bedrooms >= %s" if detect_engine() == "mysql" else " AND bedrooms >= ?"
        params.append(int(min_bedrooms))
    if max_total_rent:
        query += " AND total_rent <= %s" if detect_engine() == "mysql" else " AND total_rent <= ?"
        params.append(float(max_total_rent))

    query += " ORDER BY total_rent ASC"
    cursor.execute(query, tuple(params))
    rows = cursor.fetchall()

    results = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get("images"), str):
            try:
                d["images"] = json.loads(d["images"])
            except Exception:
                d["images"] = []
        d["rent_per_person"] = round(d["total_rent"] / max(1, d.get("ideal_roommates", d.get("bedrooms", 4))))
        results.append(d)

    conn.close()
    return results

def get_property(prop_id: str) -> Optional[Dict[str, Any]]:
    conn = get_raw_connection()
    cursor = conn.cursor()
    q = "SELECT * FROM properties WHERE id = %s" if detect_engine() == "mysql" else "SELECT * FROM properties WHERE id = ?"
    cursor.execute(q, (prop_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    if isinstance(d.get("images"), str):
        try:
            d["images"] = json.loads(d["images"])
        except Exception:
            d["images"] = []
    d["rent_per_person"] = round(d["total_rent"] / max(1, d.get("ideal_roommates", d.get("bedrooms", 4))))
    return d

def record_access_log(ip, country, country_code, region, city, latitude, longitude, path, method, status_code, user_agent, device_type):
    try:
        conn = get_raw_connection()
        cursor = conn.cursor()
        engine = detect_engine()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if engine == "mysql":
            cursor.execute("""
            INSERT INTO access_logs (ip, country, country_code, region, city, latitude, longitude, path, method, status_code, user_agent, device_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (ip, country, country_code, region, city, latitude, longitude, path, method, status_code, user_agent, device_type))
        else:
            cursor.execute("""
            INSERT INTO access_logs (ip, country, country_code, region, city, latitude, longitude, path, method, status_code, user_agent, device_type, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (ip, country, country_code, region, city, latitude, longitude, path, method, status_code, user_agent, device_type, now))
        conn.commit()
        conn.close()
    except Exception:
        pass

def get_access_logs(limit: int = 100) -> List[Dict[str, Any]]:
    conn = get_raw_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM access_logs ORDER BY id DESC LIMIT {int(limit)}")
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def record_whatsapp_log(sender_phone, sender_name, message_body, response_body):
    try:
        conn = get_raw_connection()
        cursor = conn.cursor()
        engine = detect_engine()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if engine == "mysql":
            cursor.execute("""
            INSERT INTO whatsapp_logs (sender_phone, sender_name, message_body, response_body)
            VALUES (%s, %s, %s, %s)
            """, (sender_phone, sender_name, message_body, response_body))
        else:
            cursor.execute("""
            INSERT INTO whatsapp_logs (sender_phone, sender_name, message_body, response_body, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """, (sender_phone, sender_name, message_body, response_body, now))
        conn.commit()
        conn.close()
    except Exception:
        pass

def get_whatsapp_logs(limit: int = 50) -> List[Dict[str, Any]]:
    conn = get_raw_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM whatsapp_logs ORDER BY id DESC LIMIT {int(limit)}")
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]
