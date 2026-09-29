with flattened as (select id, host, latency_ms, k, value from app_logs lateral view outer explode(context) li as k, value) select id, host, k from flattened order by k desc nulls last
