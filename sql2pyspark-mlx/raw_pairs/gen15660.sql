with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        tier_value
    from price_tiers
    lateral view explode(tier_limits) x as tier_value
)
-- Window metrics over several different partitions
select
    id,
    plan,
    count(tier_value) over w_grp plan_count_tier_value,
    stddev_samp(tier_value) over w_grp plan_stddev_samp_tier_value,
    avg(tier_value) over w_grp plan_avg_tier_value
from exploded
window w_grp as (partition by plan)
