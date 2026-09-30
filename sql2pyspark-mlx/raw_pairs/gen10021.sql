with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        v
    from app_logs
    lateral view outer explode(context) li as k, v
),
parsed as (
    -- Semi-structured parsing
    select
        latency_ms,
        status_code,
        k,
        v,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 1) != 0 as int) as has_fault
    from flattened
),
valid as (
    -- Basic cleansing and defaults
    select
        k,
        v,
        has_fault,
        round(latency_ms * 0.9, 2) as latency_ms_adjusted,
        substring(k, 1, 4) as k_prefix
    from parsed
    where k is not null
      and status_code between 268 and 425
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by k order by has_fault desc nulls last) as has_fault_dense_rank,
        avg(has_fault) over (partition by k) as k_avg_has_fault,
        rank() over (partition by k order by has_fault desc) as has_fault_rank,
        percent_rank() over (order by has_fault nulls last) as has_fault_percent_rank
    from valid
    where k = 'trace'
)
-- Grouped metrics
select
    k,
    sum(case when k = 'region' then has_fault_percent_rank else 0 end) as region_has_fault_percent_rank,
    avg(has_fault_dense_rank) filter (where k = 'version') as version_avg_has_fault_dense_rank,
    sum(case when k = 'region' then k_avg_has_fault else 0 end) as region_k_avg_has_fault,
    grouping_id() as gid
from with_windows
group by cube (k)
having sum(case when k = 'region' then k_avg_has_fault else 0 end) < 1000000
order by region_k_avg_has_fault desc nulls last
