with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        level,
        status_code,
        attr_key,
        v
    from app_logs
    lateral view outer explode(context) e as attr_key, v
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        case when level = 'INFO' then 1 else 0 end as is_info,
        coalesce(attr_key, 'UNKNOWN') as attr_key_clean
    from flattened
    where attr_key is not null
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(is_info, 0) as is_info_rounded,
        coalesce(attr_key, 'DIRECT') as attr_key_filled
    from valid
    where host = 'h2'
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        service,
        host,
        level,
        is_info,
        attr_key_clean,
        dense_rank() over (order by is_info_rounded) as is_info_rounded_dense_rank,
        avg(is_info_rounded) over (partition by host) as host_avg_is_info_rounded,
        rank() over (order by status_code) as status_code_rank,
        dense_rank() over (order by is_info_rounded desc) as is_info_rounded_dense_rank_2
    from filtered
    where is_info_rounded >= 29
),
totals as (
    -- Aggregate with conditional metrics
    select
        case when status_code_rank >= 7 then 'PREMIUM' else 'STANDARD' end as status_code_rank_tier,
        sum(case when service = 'checkout' then status_code_rank else 0 end) as checkout_status_code_rank,
        round(sum(is_info_rounded_dense_rank_2) / nullif(count(case when service = 'billing' then 1 end), 0), 2) as is_info_rounded_dense_rank_2_per_billing,
        round(avg(is_info_rounded_dense_rank_2), 2) as avg_is_info_rounded_dense_rank_2,
        sum(case when level = 'INFO' then host_avg_is_info_rounded else 0 end) as info_host_avg_is_info_rounded,
        percentile_approx(is_info_rounded_dense_rank, 0.95) as is_info_rounded_dense_rank_p95
    from enriched
    group by 1
    having sum(case when level = 'INFO' then host_avg_is_info_rounded else 0 end) < 1000000
)
select
    status_code_rank_tier,
    checkout_status_code_rank,
    is_info_rounded_dense_rank_2_per_billing,
    info_host_avg_is_info_rounded,
    is_info_rounded_dense_rank_p95
from totals
order by is_info_rounded_dense_rank_p95 desc nulls first;
