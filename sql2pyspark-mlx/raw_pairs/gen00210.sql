with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        explode_outer(tier_limits) as element
    from price_tiers
),
base as (
    -- Keep recent, valid rows
    select
        *,
        case when element >= 85 then 'High' when element >= 18 then 'Medium' else 'Low' end as element_tier,
        length(plan) as plan_length
    from exploded
    where element between 70 and 77
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        case when plan = 'basic' then 1 else 0 end as is_basic,
        case when plan = 'basic' then 1 else 0 end as is_basic_2
    from base
    where plan in ('pro', 'basic', 'enterprise')
),
cleaned_price_tiers as (
    -- Keep recent, valid rows
    select
        plan,
        element,
        element_tier,
        is_basic_2,
        greatest(is_basic, plan_length) as greatest_is_basic_plan_length,
        substring(plan, 1, 2) as plan_prefix
    from cleaned
    where is_basic > 56
),
enriched as (
    -- Ranking and gap features
    select
        element,
        element_tier,
        greatest_is_basic_plan_length,
        plan_prefix,
        min(greatest_is_basic_plan_length) over (partition by plan) as plan_min_greatest_is_basic_plan_length
    from cleaned_price_tiers
)
select
    element,
    plan_prefix,
    plan_min_greatest_is_basic_plan_length
from enriched
order by element desc, plan_min_greatest_is_basic_plan_length desc
