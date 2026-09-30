with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        log_ts,
        level,
        status_code,
        key,
        v
    from app_logs
    lateral view explode(context) t as key, v
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        key,
        v,
        max(status_code) over w_part as running_max_status_code,
        avg(status_code) over w_part as running_avg_status_code,
        sum(status_code) over w_part as running_sum_status_code,
        count(status_code) over w_part as running_count_status_code,
        avg(status_code) over (partition by key order by log_ts asc, id asc, key asc rows between 2 preceding and 1 preceding) as status_code_avg_2_prior
    from flattened
    where to_date(log_ts) >= '2023-07-30'
    window w_part as (partition by level order by log_ts asc, id asc, key asc rows between unbounded preceding and current row)
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        key,
        v,
        running_avg_status_code,
        running_sum_status_code,
        status_code_avg_2_prior,
        running_count_status_code - running_avg_status_code as running_count_status_code_minus_running_avg_status_code,
        coalesce(key, 'DIRECT') as key_clean
    from enriched
    where running_avg_status_code < 280
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        key,
        v,
        running_avg_status_code,
        status_code_avg_2_prior,
        cast(running_sum_status_code as int) as running_sum_status_code_int,
        running_avg_status_code / nullif(running_count_status_code_minus_running_avg_status_code, 0) as running_avg_status_code_per_running_count_status_code_minus_running_avg_status_code,
        coalesce(key, 'OTHER') as key_filled
    from valid
    where running_sum_status_code between 800 and 1600
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        max(running_avg_status_code_per_running_count_status_code_minus_running_avg_status_code) over (partition by key) as key_max_running_avg_status_code_per_running_count_status_code_minus_running_avg_status_code,
        lag(running_avg_status_code, 2) over (partition by key order by running_sum_status_code_int asc, id asc, key asc) as prev_running_avg_status_code,
        round(running_avg_status_code / sum(running_avg_status_code) over (partition by key), 4) as running_avg_status_code_share
    from filtered
),
valid_app_logs as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        key,
        v,
        running_avg_status_code,
        status_code_avg_2_prior,
        running_avg_status_code_per_running_count_status_code_minus_running_avg_status_code,
        key_filled,
        running_avg_status_code_share,
        floor(status_code_avg_2_prior) as status_code_avg_2_prior_whole,
        prev_running_avg_status_code * running_sum_status_code_int as prev_running_avg_status_code_times_running_sum_status_code_int
    from metrics
    where key is not null
      and key is not distinct from 'trace'
),
filtered_app_logs as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        key,
        v,
        running_avg_status_code,
        status_code_avg_2_prior,
        running_avg_status_code_per_running_count_status_code_minus_running_avg_status_code,
        key_filled,
        status_code_avg_2_prior_whole,
        cast(running_avg_status_code_per_running_count_status_code_minus_running_avg_status_code as int) as running_avg_status_code_per_running_count_status_code_minus_running_avg_status_code_int
    from valid_app_logs
    where key is not null
      and status_code_avg_2_prior_whole > 29
)
select
    id,
    key,
    v,
    status_code_avg_2_prior,
    key_filled
from filtered_app_logs
order by key desc nulls last, status_code_avg_2_prior asc;
