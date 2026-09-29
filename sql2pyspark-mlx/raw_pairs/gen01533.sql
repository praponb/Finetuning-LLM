with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view explode(context) t as attr_key, attr_value
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by level order by log_ts desc, id, attr_key) as latest_rank
    from flattened
    qualify latest_rank = 1
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(log_ts) over (partition by level order by log_ts, id, attr_key) is null
                 or log_ts > lag(log_ts) over (partition by level order by log_ts, id, attr_key) + interval 15 minutes then 1 else 0 end as new_group_flag
    from deduped
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by level order by log_ts, id, attr_key rows between unbounded preceding and current row) as session_no
    from gaps
),
filtered as (
    -- Keep recent, valid rows
    select *, day(log_ts) log_ts_day
    from islands
    where log_ts >= timestamp '2025-06-01 14:00:00'
      and attr_key is not null
)
-- Native SQL PIVOT
select *
from (
    select
        level,
        host,
        attr_key,
        new_group_flag
    from filtered
) src
pivot (
    max(new_group_flag) as amount
    for attr_key in ('trace' as trace, 'version' as version)
)
