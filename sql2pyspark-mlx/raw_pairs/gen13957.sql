with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        status_code,
        k,
        v
    from app_logs
    lateral view outer explode(context) e as k, v
),
parsed as (
    -- Struct fields, map lookups and JSON extraction
    select
        v,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 8) != 0 as int) as flag_bit8
    from exploded
),
base as (
    -- Keep recent, valid rows
    select *, flag_bit8 + status_code_high as flag_bit8_plus_status_code_high
    from parsed
    where status_code_high = 1
),
scored as (
    -- Running and moving aggregates
    select *, cume_dist() over (order by flag_bit8_plus_status_code_high desc nulls first) as flag_bit8_plus_status_code_high_cume_dist
    from base
),
with_windows as (
    -- Running and moving aggregates
    select *, dense_rank() over (order by flag_bit8_plus_status_code_high_cume_dist nulls last) as flag_bit8_plus_status_code_high_cume_dist_dense_rank
    from scored
    where flag_bit8 > 1
),
base_app_logs as (
    -- Basic cleansing and defaults
    select
        *,
        status_code_high + flag_bit8_plus_status_code_high_cume_dist_dense_rank as status_code_high_plus_flag_bit8_plus_status_code_high_cume_dist_dense_rank,
        cast(status_code_high as string) as status_code_high_str
    from with_windows
    where flag_bit8_plus_status_code_high < 36
)
select
    v,
    status_code_high,
    flag_bit8,
    flag_bit8_plus_status_code_high_cume_dist,
    flag_bit8_plus_status_code_high_cume_dist_dense_rank
from base_app_logs
order by flag_bit8_plus_status_code_high_cume_dist_dense_rank
