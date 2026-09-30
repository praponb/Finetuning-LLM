with lines as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        value
    from app_logs
    lateral view explode(context) li as k, value
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        hour(log_ts) as log_ts_hour,
        coalesce(k, 'DIRECT') as k_clean
    from lines
    where k is not null
),
scored as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by k, level order by status_code, id, k) as seq_no,
        sum(status_code) over (order by status_code, id, k rows between unbounded preceding and current row) as cumulative_status_code,
        percent_rank() over (partition by host, k order by log_ts_hour desc) as log_ts_hour_percent_rank,
        first_value(log_ts_hour, true) over (partition by k order by status_code, id, k rows between unbounded preceding and current row) as first_known_log_ts_hour,
        count(log_ts_hour) over (partition by level order by cast(log_ts as bigint) range between 3600 preceding and current row) as log_ts_hour_count_1h
    from base
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(first_known_log_ts_hour) over w_ordered as level_stddev_samp_first_known_log_ts_hour,
        dense_rank() over w_part as status_code_dense_rank,
        cume_dist() over w_recent as log_ts_hour_cume_dist,
        row_number() over w_grp as row_seq,
        max(first_known_log_ts_hour) over (order by cast(log_ts as bigint) range between 1800 preceding and current row) as first_known_log_ts_hour_max_30m
    from scored
    where log_ts_hour_count_1h > 12
    window w_ordered as (partition by level, k), w_part as (order by status_code), w_recent as (partition by level order by log_ts_hour nulls last), w_grp as (partition by host, level order by log_ts_hour desc, id, k)
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        date_trunc('week', log_ts) as log_ts_period,
        coalesce(k, 'UNKNOWN') as k_clean_2
    from metrics
    where k is not null
      and k is distinct from 'region'
)
select
    id,
    host,
    log_ts,
    level,
    status_code,
    k,
    log_ts_hour,
    k_clean,
    cumulative_status_code,
    case when level rlike '^(INFO|ERROR)$' then 'GROUP_B' else 'OTHER_ZONE' end as level_zone
from valid;
