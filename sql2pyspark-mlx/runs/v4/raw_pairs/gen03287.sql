with flattened as (select id, service, host, log_ts, k, v from app_logs lateral view outer explode(context) x as k, v) select id, host, log_ts, k from flattened
