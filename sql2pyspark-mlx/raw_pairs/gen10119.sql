with flattened as (
    select
        id,
        host,
        log_ts,
        status_code,
        key,
        value
    from app_logs
    lateral view outer explode(context) x as key, value
),
unpacked as (
    select
        host,
        status_code,
        key,
        value,
        cast((status_code & 1) != 0 as int) as has_fault,
        shiftright(status_code, 2) as status_code_high
    from flattened
),
enriched as (
    select
        *,
        percent_rank() over (partition by host order by status_code) as status_code_percent_rank,
        cume_dist() over (partition by key order by has_fault desc nulls last) as has_fault_cume_dist
    from unpacked
    where has_fault < 1
),
valid as (
    select
        *,
        has_fault_cume_dist / nullif(status_code_percent_rank, 0) as has_fault_cume_dist_per_status_code_percent_rank,
        coalesce(key, 'UNKNOWN') as key_clean
    from enriched
    where key is not null
)
select
    key,
    sum(case when key = 'trace' then status_code_high else 0 end) as trace_status_code_high,
    sum(case when key = 'version' then status_code_percent_rank else 0 end) as version_status_code_percent_rank,
    count_if(key = 'version') as version_events
from valid
group by key
order by trace_status_code_high;
