with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        value
    from app_logs
    lateral view explode(context) t as attr_key, value
),
flattened_attrs as (
    -- Higher-order array functions
    select
        id,
        latency_ms,
        attr_key,
        cast((status_code & 4) != 0 as int) as has_fault,
        shiftright(status_code, 2) as status_code_high
    from exploded
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        latency_ms,
        attr_key,
        upper(attr_key) as attr_key_upper
    from flattened_attrs
    where attr_key is not null
      and status_code_high <> 1
),
scored as (
    -- Ranking and gap features
    select
        *,
        cume_dist() over w_recent as latency_ms_cume_dist,
        sum(latency_ms) over (order by latency_ms, id, attr_key rows between unbounded preceding and current row) as cumulative_latency_ms
    from cleaned
    window w_recent as (partition by attr_key order by latency_ms desc nulls first)
)
select
    id,
    latency_ms,
    attr_key,
    latency_ms_cume_dist,
    case when attr_key rlike '^(trace|version)$' then 'GROUP_B' when attr_key rlike '^(region)$' then 'EXTENDED' else 'PRIORITY' end as attr_key_zone
from scored
order by latency_ms desc nulls last, id
