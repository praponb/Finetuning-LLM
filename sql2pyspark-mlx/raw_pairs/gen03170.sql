with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        log_ts,
        level,
        attr_key,
        value
    from app_logs
    lateral view outer explode(context) li as attr_key, value
)
-- Grouped metrics
select
    date_trunc('day', log_ts) as activity_day,
    level,
    count_if(level = 'DEBUG') as debug_events,
    min(log_ts) as first_log_ts,
    count(*) as row_count,
    min(log_ts) as started_at,
    count(distinct id) as distinct_ids,
    count(*) as event_count,
    percent_rank() over (partition by date_trunc('day', log_ts) order by count_if(level = 'DEBUG') desc) as debug_events_percent_rank
from exploded
group by date_trunc('day', log_ts), level;
