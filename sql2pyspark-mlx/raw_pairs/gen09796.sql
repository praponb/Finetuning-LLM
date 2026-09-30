with lines as (
    -- Unnest the array into one row per element
    select
        id,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    from app_logs
    lateral view explode(context) e as attr_key, v
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        round(status_code / sum(status_code) over (partition by attr_key), 4) status_code_share,
        corr(latency_ms, status_code) over (partition by attr_key) corr_latency_ms_status_code,
        ntile(10) over (partition by attr_key order by status_code desc, id, attr_key) status_code_bucket
    from lines
    where level = 'DEBUG'
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by attr_key, level order by status_code_share desc nulls last, id, attr_key) as rn
    from enriched
    qualify rn = 1
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        latency_ms,
        status_code,
        attr_key,
        v,
        corr_latency_ms_status_code,
        round(status_code_bucket / sum(status_code_bucket) over (partition by level, attr_key), 4) status_code_bucket_share,
        first_value(corr_latency_ms_status_code) ignore nulls over (partition by attr_key order by latency_ms, id, attr_key rows between unbounded preceding and current row) first_known_corr_latency_ms_status_code,
        sum(status_code) over (order by status_code, id, attr_key rows between unbounded preceding and current row) cumulative_status_code,
        cume_dist() over (order by status_code_share desc nulls first) status_code_share_cume_dist,
        last_value(status_code_share, true) over w_grp last_known_status_code_share
    from latest
    window w_grp as (order by status_code_share, id, attr_key rows between unbounded preceding and current row)
),
metrics_app_logs as (
    -- Ranking and gap features
    select
        *,
        lag(latency_ms) over (partition by attr_key order by last_known_status_code_share, id, attr_key) prev_latency_ms,
        lag(status_code_bucket_share) over (partition by attr_key order by last_known_status_code_share, id, attr_key) prev_status_code_bucket_share,
        lead(status_code_bucket_share) over (partition by attr_key order by last_known_status_code_share, id, attr_key) next_status_code_bucket_share,
        row_number() over (partition by attr_key order by last_known_status_code_share, id, attr_key) seq_no
    from metrics
),
scored as (
    -- Ranking and gap features
    select
        id,
        status_code,
        attr_key,
        corr_latency_ms_status_code,
        status_code_bucket_share,
        cumulative_status_code,
        status_code_share_cume_dist,
        last_known_status_code_share,
        prev_status_code_bucket_share,
        ntile(4) over (order by prev_latency_ms desc, id, attr_key) prev_latency_ms_bucket,
        avg(status_code_bucket_share) over (partition by attr_key order by status_code_share_cume_dist, id, attr_key rows between 6 preceding and current row) status_code_bucket_share_avg_6,
        percent_rank() over (partition by attr_key order by status_code_bucket_share desc nulls first) status_code_bucket_share_percent_rank
    from metrics_app_logs
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        cumulative_status_code - last_known_status_code_share cumulative_status_code_minus_last_known_status_code_share,
        last_known_status_code_share - status_code_bucket_share_percent_rank last_known_status_code_share_minus_status_code_bucket_share_percent_rank
    from scored
    where status_code_bucket_share <= 0.29
),
totals as (
    -- Aggregate with conditional metrics
    select
        attr_key,
        round(sum(prev_latency_ms_bucket) / nullif(count(case when attr_key = 'region' then 1 end), 0), 2) as prev_latency_ms_bucket_per_region,
        round(avg(prev_status_code_bucket_share), 2) as avg_prev_status_code_bucket_share
    from base
    group by attr_key
)
select
    attr_key,
    prev_latency_ms_bucket_per_region,
    avg_prev_status_code_bucket_share,
    case when attr_key rlike '^(region|trace)$' then 'OTHER_ZONE' else 'GROUP_B' end as attr_key_zone
from totals
order by avg_prev_status_code_bucket_share nulls last
