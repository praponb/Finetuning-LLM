with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        val
    from price_tiers
    lateral view explode(tier_prices) x as val
)
-- Multi-level aggregation
select
    case when val >= 86 then 'HIGH' else 'LOW' end as val_tier,
    plan,
    min(val) as min_val,
    count(case when plan = 'enterprise' then 1 end) as enterprise_count,
    count_if(plan = 'enterprise') as enterprise_events,
    count_if(plan = 'basic') as basic_events,
    sort_array(collect_set(plan)) as plan_list
from exploded
group by val_tier, plan
