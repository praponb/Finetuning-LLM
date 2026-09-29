with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view outer explode(context) li as attr_key, attr_value
),
scored as (
    -- Ranking and gap features
    select
        *,
        count(status_code) over (order by cast(log_ts as bigint) range between 1800 preceding and current row) as status_code_count_30m,
        max(latency_ms) over (partition by host order by cast(log_ts as bigint) range between 900 preceding and current row) as latency_ms_max_15m,
        corr(latency_ms, status_code) over (partition by attr_key) as corr_latency_ms_status_code,
        cume_dist() over (order by status_code desc) as status_code_cume_dist,
        percent_rank() over (order by latency_ms nulls last) as latency_ms_percent_rank
    from exploded
),
scored_app_logs as (
    -- Running and moving aggregates
    select
        *,
        max(status_code_cume_dist) over (partition by host order by cast(log_ts as bigint) range between 900 preceding and current row) as status_code_cume_dist_max_15m,
        sum(latency_ms_max_15m) over (order by cast(log_ts as bigint) range between 3600 preceding and current row) as latency_ms_max_15m_sum_1h,
        cume_dist() over (order by status_code_count_30m desc) as status_code_count_30m_cume_dist,
        max(latency_ms) over (partition by attr_key order by cast(log_ts as bigint) range between 900 preceding and current row) as latency_ms_max_15m_2
    from scored
    where status_code_count_30m <= 10
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_value,
        status_code_count_30m,
        latency_ms_max_15m,
        corr_latency_ms_status_code,
        status_code_cume_dist,
        latency_ms_percent_rank,
        status_code_cume_dist_max_15m,
        latency_ms_max_15m_sum_1h,
        status_code_count_30m_cume_dist,
        latency_ms_max_15m_2,
        round(status_code_cume_dist, 0) as status_code_cume_dist_rounded,
        coalesce(attr_key, 'OTHER') as resolved_attr_key
    from scored_app_logs
    where log_ts > to_date('2025-06-01')
      and attr_key is not null
)
-- Native SQL UNPIVOT (wide to long)
select
    service,
    metric_name,
    metric_value
from (
    select
        service,
        status_code_cume_dist,
        status_code_cume_dist_rounded,
        corr_latency_ms_status_code,
        latency_ms_percent_rank
    from cleaned
) src
unpivot include nulls (
    metric_value for metric_name in (status_code_cume_dist as STATUS_CODE_CUME_DIST, status_code_cume_dist_rounded, corr_latency_ms_status_code as CORR_LATENCY_MS_STATUS_CODE, latency_ms_percent_rank as LATENCY_MS_PERCENT_RANK)
);
