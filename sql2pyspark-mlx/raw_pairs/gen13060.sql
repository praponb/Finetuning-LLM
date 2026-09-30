with lines as (
    select
        id,
        plan,
        effective_from,
        val
    from price_tiers
    lateral view explode(discounts) e as val
),
base as (
    select
        *,
        coalesce(val, 0) as val_filled,
        substring(plan, 1, 4) as plan_prefix
    from lines
    where val between 48 and 57
),
enriched as (
    select
        *,
        cume_dist() over (partition by plan order by val_filled desc nulls last) as val_filled_cume_dist,
        min(val_filled) over (partition by plan) as plan_min_val_filled
    from base
),
enriched_price_tiers as (
    select
        *,
        percent_rank() over (partition by plan order by val_filled_cume_dist) as val_filled_cume_dist_percent_rank,
        dense_rank() over (order by val desc nulls last) as val_dense_rank
    from enriched
),
grouped as (
    select
        date_trunc('quarter', effective_from) as quarter_start,
        approx_percentile(plan_min_val_filled, array(0.25, 0.5, 0.75)) as plan_min_val_filled_quartiles,
        count(*) as event_count
    from enriched_price_tiers
    group by date_trunc('quarter', effective_from)
)
select quarter_start
from grouped
order by quarter_start nulls last
