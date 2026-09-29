with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        explode_outer(tier_prices) as val
    from price_tiers
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        max(val) over (partition by plan) as plan_max_val,
        cume_dist() over (order by val desc nulls last) as val_cume_dist,
        rank() over (partition by plan order by val desc) as val_rank
    from flattened
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        plan_max_val,
        val_cume_dist,
        val_rank,
        last_day(effective_from) as effective_from_month_end
    from metrics
    where effective_from >= to_date('2024-03-07')
      and plan_max_val = 62
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        plan_max_val,
        val_cume_dist,
        percent_rank() over (order by plan_max_val desc nulls first) as plan_max_val_percent_rank
    from filtered
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        rank() over (order by val_cume_dist desc nulls first) as val_cume_dist_rank,
        percent_rank() over (order by val_cume_dist asc nulls first) as val_cume_dist_percent_rank,
        rank() over (order by plan_max_val_percent_rank desc) as plan_max_val_percent_rank_rank,
        dense_rank() over (order by plan_max_val_percent_rank desc nulls last) as plan_max_val_percent_rank_dense_rank
    from enriched
),
base as (
    -- Keep recent, valid rows
    select *, val_cume_dist_percent_rank / nullif(plan_max_val_percent_rank, 0) as val_cume_dist_percent_rank_per_plan_max_val_percent_rank
    from with_windows
    where val_cume_dist_rank > 7
),
totals as (
    -- Aggregate with conditional metrics
    select
        case when plan_max_val_percent_rank_dense_rank >= 9 then 'PREMIUM' else 'STANDARD' end as plan_max_val_percent_rank_dense_rank_tier,
        percentile_approx(plan_max_val, array(0.25, 0.5, 0.75)) as plan_max_val_quartiles,
        count(*) as row_count,
        count(distinct id) as distinct_ids
    from base
    group by case when plan_max_val_percent_rank_dense_rank >= 9 then 'PREMIUM' else 'STANDARD' end
)
select
    plan_max_val_percent_rank_dense_rank_tier,
    row_count,
    distinct_ids
from totals;
