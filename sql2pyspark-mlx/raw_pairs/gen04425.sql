with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) e as k, attr_value
),
filtered as (
    -- Basic cleansing and defaults
    select
        host,
        log_ts,
        status_code,
        attr_value,
        round(status_code * 100, 2) as adjusted_status_code
    from lines
    where status_code <= 319
),
metrics as (
    -- Running and moving aggregates
    select
        host,
        log_ts,
        status_code,
        attr_value,
        adjusted_status_code,
        min(adjusted_status_code) over (partition by host) as host_min_adjusted_status_code,
        percent_rank() over (order by adjusted_status_code desc nulls last) as adjusted_status_code_percent_rank,
        max(adjusted_status_code) over (partition by host) as host_max_adjusted_status_code
    from filtered
),
cleaned as (
    -- Basic cleansing and defaults
    select *, floor(adjusted_status_code_percent_rank) as adjusted_status_code_percent_rank_whole
    from metrics
    where adjusted_status_code_percent_rank is not null
),
base as (
    -- Keep recent, valid rows
    select
        host,
        status_code,
        attr_value,
        adjusted_status_code,
        host_min_adjusted_status_code,
        adjusted_status_code_percent_rank,
        host_max_adjusted_status_code,
        initcap(trim(host)) as host_title
    from cleaned
    where log_ts >= timestamp '2025-06-01 12:00:00'
      and host_max_adjusted_status_code < 54
)
-- Multi-level aggregation
select
    host,
    round(avg(host_min_adjusted_status_code), 2) as avg_host_min_adjusted_status_code,
    percentile_approx(adjusted_status_code, array(0.25, 0.5, 0.75)) as adjusted_status_code_quartiles,
    count_if(host = 'h2') as h2_events,
    sum(adjusted_status_code) filter (where host = 'h1') as h1_total_adjusted_status_code,
    round(avg(host_min_adjusted_status_code), 2) as avg_host_min_adjusted_status_code_2,
    count(distinct host) as distinct_hosts
from base
group by host
order by distinct_hosts nulls first;
