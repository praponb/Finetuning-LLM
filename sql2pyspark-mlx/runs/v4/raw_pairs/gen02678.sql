with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) e as key, attr_value
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        attr_value,
        greatest(latency_ms, status_code) greatest_latency_ms_status_code,
        coalesce(key, 'N/A') key_clean
    from exploded
    where log_ts > to_date('2025-06-01')
),
scored as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(latency_ms) over w_hist host_stddev_samp_latency_ms,
        count(greatest_latency_ms_status_code) over w_recent key_count_greatest_latency_ms_status_code,
        corr(status_code, latency_ms) over w_hist corr_status_code_latency_ms
    from cleaned
    window w_hist as (partition by host), w_recent as (partition by key, host)
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        avg(key_count_greatest_latency_ms_status_code) over (partition by key order by cast(log_ts as bigint) range between 900 preceding and current row) key_count_greatest_latency_ms_status_code_avg_15m,
        sum(key_count_greatest_latency_ms_status_code) over (order by cast(log_ts as bigint) range between 1800 preceding and current row) key_count_greatest_latency_ms_status_code_sum_30m
    from scored
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        key,
        greatest_latency_ms_status_code,
        corr_status_code_latency_ms
    from with_windows
) src
unpivot include nulls (
    val for kpi in (greatest_latency_ms_status_code as GREATEST_LATENCY_MS_STATUS_CODE, corr_status_code_latency_ms as CORR_STATUS_CODE_LATENCY_MS_VAL)
);
