with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        value
    from app_logs
    lateral view outer explode(context) t as key, value
),
enriched as (
    -- Running and moving aggregates
    select
        service,
        log_ts,
        level,
        avg(latency_ms) over (partition by key) as key_avg_latency_ms,
        corr(latency_ms, status_code) over w_ordered as corr_latency_ms_status_code
    from flattened
    where latency_ms between 1060 and 1335
    window w_ordered as (partition by level)
),
with_windows as (
    -- Ranking and gap features
    select
        log_ts,
        level,
        key_avg_latency_ms,
        corr_latency_ms_status_code,
        percent_rank() over (order by corr_latency_ms_status_code desc) as corr_latency_ms_status_code_percent_rank,
        avg(corr_latency_ms_status_code) over (order by cast(log_ts as bigint) range between 1800 preceding and current row) as corr_latency_ms_status_code_avg_30m
    from enriched
),
metrics as (
    -- Running and moving aggregates
    select
        level,
        key_avg_latency_ms,
        corr_latency_ms_status_code,
        corr_latency_ms_status_code_percent_rank,
        avg(corr_latency_ms_status_code_percent_rank) over (partition by level order by cast(log_ts as bigint) range between 900 preceding and current row) as corr_latency_ms_status_code_percent_rank_avg_15m
    from with_windows
)
-- Multi-level aggregation
select
    level,
    sum(corr_latency_ms_status_code_percent_rank_avg_15m) filter (where level = 'ERROR') as error_total_corr_latency_ms_status_code_percent_rank_avg_15m,
    count(distinct level) as distinct_levels
from metrics
group by level
order by level
