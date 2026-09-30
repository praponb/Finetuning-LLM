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
        v
    from app_logs
    lateral view explode(context) x as attr_key, v
),
unpacked as (
    -- Higher-order array functions
    select
        id,
        host,
        log_ts,
        status_code,
        attr_key,
        cast((status_code & 4) != 0 as int) as flag_bit4,
        shiftright(status_code, 2) as status_code_high
    from exploded
    where status_code >= 404
),
remaining_keys as (
    -- Set intersection on keys
    select id
    from unpacked
    intersect
    select id
    from app_logs
    where status_code != 426
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from unpacked s
    inner join remaining_keys k on s.id = k.id
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        host,
        log_ts,
        status_code,
        attr_key,
        flag_bit4,
        status_code_high,
        initcap(trim(host)) as host_title,
        length(host) as host_length,
        coalesce(attr_key, 'DIRECT') as resolved_attr_key
    from verified
    where log_ts >= date '2025-06-02'
      and attr_key = 'region'
)
select
    id,
    log_ts,
    status_code,
    attr_key,
    status_code_high,
    host_title,
    host_length,
    case when attr_key rlike '^(region|trace)$' then 'OTHER_ZONE' when attr_key rlike '^(version)$' then 'CORE' else 'EXTENDED' end as attr_key_zone
from valid
order by attr_key
