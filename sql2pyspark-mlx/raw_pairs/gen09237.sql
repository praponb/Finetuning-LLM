with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        tier_value
    from price_tiers
    lateral view outer explode(discounts) e as tier_value
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        effective_from,
        dense_rank() over (order by tier_value desc nulls last) as tier_value_dense_rank,
        dense_rank() over (partition by date_trunc('MONTH', effective_from), currency order by tier_value desc nulls first) as tier_value_dense_rank_2
    from flattened
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over w_grp as tier_value_dense_rank_dense_rank,
        rank() over (order by tier_value_dense_rank desc) as tier_value_dense_rank_rank,
        dense_rank() over (partition by year(effective_from) order by tier_value_dense_rank desc nulls last) as tier_value_dense_rank_dense_rank_2
    from enriched
    window w_grp as (order by tier_value_dense_rank asc nulls first)
),
base as (
    -- Basic cleansing and defaults
    select
        effective_from,
        tier_value_dense_rank,
        tier_value_dense_rank_2,
        tier_value_dense_rank_dense_rank,
        tier_value_dense_rank_dense_rank_2,
        date_trunc('WEEK', effective_from) as effective_from_period,
        cast(tier_value_dense_rank_2 as double) as tier_value_dense_rank_2_num
    from with_windows
    where effective_from > date '2024-06-15'
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, round(tier_value_dense_rank, 0) as tier_value_dense_rank_rounded
    from base
    where effective_from >= date '2024-06-01'
      and effective_from < '2024-10-01'
)
-- Multi-level aggregation
select
    date_trunc('month', effective_from) as activity_month,
    case when tier_value_dense_rank_2_num >= 85 then 'PREMIUM' else 'STANDARD' end as tier_value_dense_rank_2_num_tier,
    max(effective_from_period) as last_effective_from_period,
    count(*) as row_count,
    count(*) as cnt,
    dense_rank() over (partition by date_trunc('month', effective_from) order by count(*) desc) as cnt_dense_rank
from cleaned
group by activity_month, tier_value_dense_rank_2_num_tier;
