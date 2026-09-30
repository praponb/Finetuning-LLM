with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        latency_ms,
        status_code,
        attr_key,
        value
    from app_logs
    lateral view outer explode(context) x as attr_key, value
),
unpacked as (
    -- Higher-order array functions
    select
        id,
        latency_ms,
        cast((status_code & 1) != 0 as int) as is_flagged,
        shiftright(status_code, 2) as status_code_high
    from flattened
),
scored as (
    -- Window metrics over several different partitions
    select
        latency_ms,
        is_flagged,
        status_code_high,
        rank() over (order by status_code_high nulls last) as status_code_high_rank,
        cume_dist() over (order by status_code_high desc) as status_code_high_cume_dist,
        percent_rank() over (order by latency_ms nulls first) as latency_ms_percent_rank
    from unpacked
    where latency_ms = 1649
),
valid as (
    -- Keep recent, valid rows
    select
        latency_ms,
        is_flagged,
        status_code_high,
        status_code_high_rank,
        status_code_high_cume_dist,
        latency_ms_percent_rank,
        latency_ms_percent_rank - latency_ms as latency_ms_percent_rank_minus_latency_ms,
        status_code_high_rank / nullif(latency_ms_percent_rank, 0) as status_code_high_rank_per_latency_ms_percent_rank
    from scored
    where status_code_high_rank between 4 and 6
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        cume_dist() over (order by status_code_high_rank desc nulls last) as status_code_high_rank_cume_dist,
        percent_rank() over w_hist as status_code_high_rank_per_latency_ms_percent_rank_percent_rank,
        dense_rank() over (order by latency_ms_percent_rank desc nulls first) as latency_ms_percent_rank_dense_rank,
        dense_rank() over w_grp as status_code_high_cume_dist_dense_rank
    from valid
    window w_hist as (order by status_code_high_rank_per_latency_ms_percent_rank desc), w_grp as (order by status_code_high_cume_dist)
),
valid_app_logs as (
    -- Keep recent, valid rows
    select
        latency_ms,
        is_flagged,
        status_code_high,
        status_code_high_cume_dist,
        latency_ms_percent_rank,
        latency_ms_percent_rank_minus_latency_ms,
        status_code_high_rank_per_latency_ms_percent_rank,
        status_code_high_rank_cume_dist,
        latency_ms_percent_rank_dense_rank,
        status_code_high_cume_dist_dense_rank,
        round(status_code_high_rank_per_latency_ms_percent_rank_percent_rank, 0) as status_code_high_rank_per_latency_ms_percent_rank_percent_rank_rounded
    from metrics
    where status_code_high_rank_cume_dist >= 0.32
)
select
    latency_ms,
    status_code_high,
    latency_ms_percent_rank,
    latency_ms_percent_rank_minus_latency_ms,
    status_code_high_rank_cume_dist,
    latency_ms_percent_rank_dense_rank,
    status_code_high_cume_dist_dense_rank
from valid_app_logs
