with exploded as (
    -- Unnest the array into one row per element
    select id, val
    from price_tiers
    lateral view explode(tier_limits) x as val
),
enriched as (
    -- Ranking and gap features
    select *, dense_rank() over (order by val) as val_dense_rank
    from exploded
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, val_dense_rank * val as val_dense_rank_times_val
    from enriched
    where val_dense_rank >= 4
),
metrics as (
    -- Running and moving aggregates
    select
        val,
        val_dense_rank_times_val,
        dense_rank() over (order by val_dense_rank_times_val desc) as val_dense_rank_times_val_dense_rank,
        rank() over (order by val) as val_rank
    from cleaned
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        greatest(val_dense_rank_times_val, val) as greatest_val_dense_rank_times_val_val,
        coalesce(val_dense_rank_times_val, 0) as val_dense_rank_times_val_filled
    from metrics
    where val_dense_rank_times_val_dense_rank = 8
),
base as (
    -- Basic cleansing and defaults
    select *, cast(val_rank as string) as val_rank_num
    from valid
    where greatest_val_dense_rank_times_val_val > 23
),
stats as (
    -- Multi-level aggregation
    select
        case when val_dense_rank_times_val >= 84 then 'LARGE' else 'SMALL' end as val_dense_rank_times_val_tier,
        approx_percentile(val_dense_rank_times_val_filled, 0.5) as val_dense_rank_times_val_filled_p50,
        round(avg(val_dense_rank_times_val_filled), 2) as avg_val_dense_rank_times_val_filled,
        approx_percentile(val, array(0.25, 0.5, 0.75)) as val_quartiles
    from base
    group by case when val_dense_rank_times_val >= 84 then 'LARGE' else 'SMALL' end
)
select val_dense_rank_times_val_tier
from stats
order by val_dense_rank_times_val_tier desc nulls last;
