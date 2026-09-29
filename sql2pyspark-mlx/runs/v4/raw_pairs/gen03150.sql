with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        log_ts,
        level,
        status_code,
        key,
        value
    from app_logs
    lateral view explode(context) e as key, value
),
unpacked as (
    -- Semi-structured parsing
    select
        id,
        level,
        status_code,
        key,
        cast((status_code & 2) != 0 as int) as is_flagged,
        shiftright(status_code, 2) as status_code_high
    from flattened
),
matched_keys as (
    -- Set intersection on keys
    select level, id
    from unpacked
    intersect all
    select level, id
    from app_logs
    where host = 'h3'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from unpacked s
    inner join matched_keys k on s.level = k.level and s.id = k.id
),
metrics as (
    -- Ranking and gap features
    select
        id,
        level,
        status_code,
        key,
        is_flagged,
        status_code_high,
        max(status_code) over (partition by key) as key_max_status_code,
        round(status_code / sum(status_code) over (partition by level), 4) as status_code_share
    from verified
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        stddev_samp(status_code_high) over (partition by level) as level_stddev_samp_status_code_high,
        round(key_max_status_code / sum(key_max_status_code) over (partition by key), 4) as key_max_status_code_share,
        stddev_samp(status_code_high) over (partition by key) as key_stddev_samp_status_code_high,
        min(status_code_high) over (partition by key) as key_min_status_code_high
    from metrics
),
metrics_app_logs as (
    -- Window metrics over several different partitions
    select
        id,
        level,
        status_code,
        key,
        is_flagged,
        status_code_high,
        key_max_status_code,
        status_code_share,
        level_stddev_samp_status_code_high,
        key_max_status_code_share,
        key_stddev_samp_status_code_high,
        key_min_status_code_high,
        count(key_max_status_code_share) over (partition by level) as level_count_key_max_status_code_share,
        round(status_code / sum(status_code) over (partition by key), 4) as status_code_share_2
    from with_windows
),
metrics_app_logs_2 as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by level, key order by status_code desc) as status_code_percent_rank,
        cume_dist() over (partition by level order by key_min_status_code_high desc) as key_min_status_code_high_cume_dist,
        rank() over (partition by key order by status_code) as status_code_rank
    from metrics_app_logs
)
-- Ranking and gap features
select
    *,
    dense_rank() over (partition by key order by status_code_high nulls first) as status_code_high_dense_rank,
    min(status_code_share_2) over w_grp as level_min_status_code_share_2,
    sum(status_code_share_2) over w_grp as level_sum_status_code_share_2
from metrics_app_logs_2
window w_grp as (partition by level);
