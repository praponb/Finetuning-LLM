with flattened as (select id, host, level, attr_key, attr_value from app_logs lateral view outer explode(context) t as attr_key, attr_value) select id, host from flattened order by host desc
