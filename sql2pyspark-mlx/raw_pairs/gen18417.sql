with flattened as (
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    from app_logs
    lateral view outer explode(context) e as key, v
)
select
    host,
    date_trunc('day', log_ts) as activity_day,
    count(*) filter (where host = 'h3') as h3_rows,
    count(distinct id) as distinct_ids,
    count(*) filter (where host = 'h2') as h2_rows,
    avg(status_code) filter (where host = 'h1') as h1_avg_status_code,
    sum(case when key = 'trace' then latency_ms else 0 end) as trace_latency_ms,
    grouping_id() as grouping_level
from flattened
group by cube (host, date_trunc('day', log_ts))
order by h1_avg_status_code desc nulls first;
