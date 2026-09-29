with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view explode(context) x as key, attr_value
),
flattened_attrs as (
    -- Semi-structured parsing
    select
        id,
        host,
        key,
        cast((status_code & 8) != 0 as int) as flag_bit8,
        shiftright(status_code, 2) as status_code_high
    from exploded
),
scored as (
    -- Running and moving aggregates
    select
        id,
        host,
        key,
        status_code_high,
        lead(status_code_high) over w_recent as next_status_code_high,
        lag(status_code_high) over w_recent as prev_status_code_high,
        lag(flag_bit8) over w_recent as prev_flag_bit8,
        row_number() over w_recent as row_seq,
        cume_dist() over (partition by host order by status_code_high desc nulls first) as status_code_high_cume_dist
    from flattened_attrs
    window w_recent as (partition by key, host order by status_code_high asc, id asc, key asc)
),
latest as (
    -- Keep one row per key (latest first)
    select
        id,
        key,
        status_code_high,
        prev_flag_bit8
    from (
        select *, row_number() over (partition by host order by next_status_code_high desc nulls first, id asc, key asc) as row_num
        from scored
    ) t
    where row_num = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        lead(status_code_high) over (partition by key order by status_code_high asc, id asc, key asc) as next_status_code_high,
        lead(prev_flag_bit8) over (partition by key order by status_code_high asc, id asc, key asc) as next_prev_flag_bit8,
        lag(prev_flag_bit8) over (partition by key order by status_code_high asc, id asc, key asc) as prev_prev_flag_bit8,
        row_number() over (partition by key order by status_code_high asc, id asc, key asc) as event_seq,
        first_value(key) ignore nulls over (partition by key order by prev_flag_bit8 asc, id asc, key asc rows between unbounded preceding and current row) as first_known_key
    from latest
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from enriched
    qualify row_number() over (partition by key order by next_prev_flag_bit8 desc nulls first, id asc, key asc) = 1
),
latest_app_logs as (
    -- Deduplicate with ROW_NUMBER
    select *
    from ranked
    qualify row_number() over (partition by key order by status_code_high desc, id asc, key asc) = 1
)
select
    id,
    key,
    prev_flag_bit8,
    next_status_code_high,
    prev_prev_flag_bit8,
    event_seq
from latest_app_logs
order by next_status_code_high desc, event_seq asc;
