with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view explode(context) li as k, attr_value
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        service,
        latency_ms,
        status_code,
        k,
        substring(k, 1, 3) as k_prefix,
        trim(host) as clean_host,
        coalesce(k, 'UNKNOWN') as resolved_k
    from lines
    where k != 'region'
),
cleaned_app_logs as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        latency_ms,
        k,
        resolved_k,
        round(status_code / latency_ms, 2) as status_code_per_latency_ms
    from cleaned
    where k is not null
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        k,
        resolved_k,
        first_value(latency_ms) ignore nulls over (partition by k order by latency_ms asc, id asc, k asc rows between unbounded preceding and current row) as first_known_latency_ms,
        sum(status_code_per_latency_ms) over (partition by k order by latency_ms asc, id asc, k asc rows between unbounded preceding and current row) as cumulative_status_code_per_latency_ms,
        percent_rank() over (partition by k order by status_code_per_latency_ms desc nulls first) as status_code_per_latency_ms_percent_rank
    from cleaned_app_logs
),
summary as (
    -- Grouped metrics
    select
        k,
        case when first_known_latency_ms >= 2643 then 'PREMIUM' else 'STANDARD' end as first_known_latency_ms_tier,
        count(*) filter (where k = 'region') as region_rows,
        max(first_known_latency_ms) filter (where k = 'trace') as trace_max_first_known_latency_ms,
        approx_percentile(status_code_per_latency_ms_percent_rank, array(0.25, 0.5, 0.75)) as status_code_per_latency_ms_percent_rank_quartiles,
        count(*) filter (where k = 'region') as region_rows_2,
        round(avg(cumulative_status_code_per_latency_ms), 2) as avg_cumulative_status_code_per_latency_ms,
        percentile_approx(first_known_latency_ms, array(0.25, 0.5, 0.75)) as first_known_latency_ms_quartiles,
        dense_rank() over (partition by k order by round(avg(cumulative_status_code_per_latency_ms), 2) desc) as avg_cumulative_status_code_per_latency_ms_dense_rank
    from scored
    group by k, case when first_known_latency_ms >= 2643 then 'PREMIUM' else 'STANDARD' end
    having avg_cumulative_status_code_per_latency_ms < 1000000
)
select
    k,
    first_known_latency_ms_tier,
    region_rows,
    trace_max_first_known_latency_ms,
    region_rows_2,
    avg_cumulative_status_code_per_latency_ms,
    first_known_latency_ms_quartiles
from summary
