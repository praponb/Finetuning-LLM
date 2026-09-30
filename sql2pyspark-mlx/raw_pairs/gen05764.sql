with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        attr_key,
        v
    from app_logs
    lateral view explode(context) x as attr_key, v
),
valid as (
    -- Basic cleansing and defaults
    select *, concat_ws(',', level, host) as level_host_key
    from lines
    where log_ts >= timestamp '2025-06-01 08:00:00'
),
ranked as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by level order by log_ts) as rn
    from valid
    qualify rn = 1
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        service,
        log_ts,
        level,
        attr_key,
        v,
        level_host_key,
        trim(level) as clean_level,
        cast(rn as double) as rn_num
    from ranked
    where attr_key in ('region', 'version', 'trace')
)
select
    id,
    level,
    attr_key,
    v,
    level_host_key,
    clean_level,
    rn_num,
    case when level rlike '^(ERROR|DEBUG)$' then 'GROUP_B' when level rlike '^(INFO|WARN)$' then 'OTHER_ZONE' else 'CORE' end as level_zone
from filtered
order by attr_key desc
