with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        status_code,
        attr_key,
        v
    from app_logs
    lateral view outer explode(context) li as attr_key, v
),
parsed as (
    -- Higher-order array functions
    select
        host,
        log_ts,
        status_code,
        attr_key,
        v,
        cast((status_code & 4) != 0 as int) as is_flagged,
        shiftright(status_code, 2) as status_code_high
    from lines
),
scored as (
    -- Running and moving aggregates
    select
        status_code,
        attr_key,
        dense_rank() over (order by status_code_high desc nulls last) as status_code_high_dense_rank,
        dense_rank() over (partition by attr_key order by status_code desc) as status_code_dense_rank
    from parsed
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        round(status_code_high_dense_rank / status_code_dense_rank, 2) as status_code_high_dense_rank_per_status_code_dense_rank,
        coalesce(status_code_high_dense_rank, 0) as status_code_high_dense_rank_filled
    from scored
    where status_code_dense_rank = 8
)
-- Multi-level aggregation
select
    attr_key,
    bit_or(status_code) as status_code_bit_or,
    count(*) as event_count,
    max(status_code_dense_rank) as max_status_code_dense_rank,
    round(avg(status_code_high_dense_rank_filled), 2) as avg_status_code_high_dense_rank_filled,
    round(sum(status_code_high_dense_rank_filled) / nullif(count(case when attr_key = 'version' then 1 end), 0), 2) as status_code_high_dense_rank_filled_per_version,
    bit_and(status_code) as status_code_bit_and
from valid
group by attr_key
having bit_and(status_code) < 1000
order by status_code_bit_and;
