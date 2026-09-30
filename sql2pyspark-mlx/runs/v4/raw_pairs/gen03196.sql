with exploded as (
    select
        id,
        host,
        log_ts,
        status_code,
        key,
        value
    from app_logs
    lateral view explode(context) x as key, value
),
flattened_attrs as (
    select
        id,
        log_ts,
        status_code,
        key,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 8) != 0 as int) as flag_bit8
    from exploded
),
ranked as (
    select *, row_number() over (partition by key order by status_code desc, id asc, key asc) as dedup_rank
    from flattened_attrs
    qualify dedup_rank = 1
)
select
    id,
    log_ts,
    status_code,
    key,
    status_code_high,
    flag_bit8
from ranked
order by status_code_high desc nulls first;
