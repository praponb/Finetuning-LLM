with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) x as k, attr_value
),
scored as (
    -- Running and moving aggregates
    select
        *,
        round(latency_ms / sum(latency_ms) over (partition by k), 4) as latency_ms_share,
        round(latency_ms / sum(latency_ms) over (partition by k, service), 4) as latency_ms_share_2
    from flattened
    where status_code <= 334
),
cleaned as (
    -- Keep recent, valid rows
    select
        service,
        status_code,
        k,
        attr_value,
        latency_ms_share,
        case when service in ('checkout', 'search') then 'Tier 1' else 'Other' end as service_group,
        coalesce(k, 'DIRECT') as k_clean
    from scored
    where log_ts >= date '2025-06-01'
),
kept_keys as (
    -- Set difference on keys
    select service
    from cleaned
    except
    select service
    from app_logs
    where service not in ('billing', 'search', 'checkout')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join kept_keys k on s.service = k.service
),
base as (
    -- Basic cleansing and defaults
    select
        status_code,
        k,
        attr_value,
        service_group,
        round(status_code * 1.15, 2) as status_code_adjusted
    from verified
    where service = 'checkout'
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        lower(k) as k_lower,
        coalesce(k, 'NONE') as k_clean
    from base
    where status_code = 337
)
select
    status_code,
    k,
    attr_value,
    service_group,
    status_code_adjusted,
    k_lower,
    k_clean
from valid
order by status_code nulls first
