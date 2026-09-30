with exploded as (select id, service, host, level, status_code, key, value from app_logs lateral view outer explode(context) x as key, value) select level, status_code, key, value from exploded;
