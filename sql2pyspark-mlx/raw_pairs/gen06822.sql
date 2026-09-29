with flattened as (
    select
        id,
        log_ts,
        level,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) li as key, attr_value
),
cleaned as (
    select
        id,
        level,
        attr_value,
        trim(level) as clean_level
    from flattened
    where key is not null
),
valid as (
    select
        *,
        trim(level) as clean_level_2,
        case when level in ('INFO', 'ERROR') then 'Group A' else 'Other' end as level_group
    from cleaned
    where level not in ('WARN', 'DEBUG', 'INFO')
),
valid_app_logs as (
    select *, initcap(trim(level)) as level_title
    from valid
    where level = 'INFO'
),
cleaned_app_logs as (
    select *, case when level = 'ERROR' then 1 else 0 end as is_error
    from valid_app_logs
    where level = 'DEBUG'
),
valid_app_logs_2 as (
    select
        attr_value,
        clean_level,
        clean_level_2,
        level_group,
        level_title,
        is_error,
        upper(level) as level_upper
    from cleaned_app_logs
    where is_error <= 45
),
report as (
    select level_group, level_upper
    from valid_app_logs_2
)
select level_group
from report
