with lines as (
    select
        id,
        service,
        host,
        level,
        latency_ms,
        key,
        attr_value
    from app_logs
    lateral view explode(context) e as key, attr_value
)
select
    *,
    row_number() over (partition by service order by latency_ms desc, id, key) as seq_no,
    sum(latency_ms) over (partition by service order by latency_ms, id, key rows between unbounded preceding and current row) as cumulative_latency_ms
from lines
