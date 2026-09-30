with exploded as (
    select
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        v
    from app_logs
    lateral view explode(context) x as k, v
),
prev_events as (
    select *, lag(log_ts) over (partition by level order by log_ts, id, k) as prev_log_ts
    from exploded
),
gaps as (
    select *, case when prev_log_ts is null or log_ts > prev_log_ts + interval '20' minute then 1 else 0 end as session_start_flag
    from prev_events
),
sessions_app_logs as (
    select
        id,
        log_ts,
        level,
        status_code,
        k,
        sum(session_start_flag) over (partition by level order by log_ts, id, k rows between unbounded preceding and current row) as burst_id
    from gaps
),
gaps_2 as (
    select *, case when lag(log_ts) over (partition by k order by log_ts, id, k) is null
                 or log_ts > lag(log_ts) over (partition by k order by log_ts, id, k) + interval 45 minutes then 1 else 0 end as session_start_flag
    from sessions_app_logs
),
sessionized as (
    select *, sum(session_start_flag) over (partition by k order by log_ts, id, k rows between unbounded preceding and current row) as island_id
    from gaps_2
),
valid as (
    select
        id,
        level,
        status_code,
        k,
        session_start_flag,
        island_id,
        least(burst_id, island_id) as least_burst_id_island_id,
        case when session_start_flag >= 1 then 'High' when session_start_flag >= 0 then 'Medium' else 'Low' end as session_start_flag_tier,
        coalesce(k, 'NONE') as k_filled
    from sessionized
    where log_ts >= to_timestamp('2025-06-01 15:00:00')
      and k is not null
),
kept_keys as (
    select id, level
    from valid
    intersect
    select id, level
    from app_logs
    where level = 'WARN'
),
reconciled as (
    select s.*
    from valid s
    inner join kept_keys k on s.id = k.id and s.level = k.level
),
scored as (
    select
        id,
        session_start_flag,
        session_start_flag_tier,
        percent_rank() over (partition by level order by status_code desc) as status_code_percent_rank,
        rank() over (partition by k order by status_code) as status_code_rank,
        min(island_id) over (partition by k) as k_min_island_id,
        stddev_samp(status_code) over (partition by level, k) as level_stddev_samp_status_code
    from reconciled
    where level <> 'DEBUG'
)
select
    k_min_island_id,
    case when session_start_flag >= 1 then 'LARGE' else 'SMALL' end as session_start_flag_tier,
    round(avg(status_code_percent_rank), 2) as avg_status_code_percent_rank,
    percentile_approx(session_start_flag, array(0.25, 0.5, 0.75)) as session_start_flag_quartiles,
    min(session_start_flag) as min_session_start_flag
from scored
group by k_min_island_id, case when session_start_flag >= 1 then 'LARGE' else 'SMALL' end
having min(session_start_flag) < 1000000
order by k_min_island_id desc;
