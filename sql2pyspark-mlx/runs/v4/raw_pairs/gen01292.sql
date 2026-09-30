with exploded as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        key,
        v
    from app_logs
    lateral view explode(context) t as key, v
),
with_gaps as (
    select *, case when log_ts - lag(log_ts) over (partition by host order by log_ts, id, key) > interval 1 hour then 1 else 0 end as session_start_flag
    from exploded
),
sessionized as (
    select *, sum(session_start_flag) over (partition by host order by log_ts, id, key rows between unbounded preceding and current row) as island_id
    from with_gaps
),
with_windows as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        key,
        v,
        session_start_flag,
        island_id,
        first_value(key, true) over (partition by level order by island_id, id, key rows between unbounded preceding and current row) as first_known_key,
        ntile(4) over (partition by level, key order by session_start_flag desc, id, key) as session_start_flag_bucket,
        lead(status_code, 1, 0) over (partition by service, level order by session_start_flag, id, key) as next_status_code,
        percent_rank() over (partition by level order by island_id) as island_id_percent_rank
    from sessionized
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by host, service order by session_start_flag_bucket, id, key) as rank_in_group
        from with_windows
    ) ranked
    where rank_in_group = 1
),
daily as (
    select
        date_trunc('hour', log_ts) as hour_window,
        count(case when level = 'DEBUG' then 1 end) as debug_count,
        avg(status_code) filter (where host = 'h4') as h4_avg_status_code,
        avg(session_start_flag) filter (where key = 'trace') as trace_avg_session_start_flag,
        min(island_id_percent_rank) as min_island_id_percent_rank,
        count(case when level = 'DEBUG' then 1 end) as debug_count_2
    from ranked
    group by date_trunc('hour', log_ts)
)
select
    hour_window,
    h4_avg_status_code,
    trace_avg_session_start_flag,
    min_island_id_percent_rank
from daily
