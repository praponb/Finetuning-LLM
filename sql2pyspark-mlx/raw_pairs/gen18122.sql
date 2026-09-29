with lines as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        v
    from app_logs
    lateral view explode(context) li as k, v
)
select
    *,
    lead(latency_ms) over (partition by service, k order by latency_ms, id, k) as next_latency_ms,
    row_number() over (partition by service, k order by latency_ms, id, k) as event_seq
from lines
where service not in ('search', 'auth', 'billing');
