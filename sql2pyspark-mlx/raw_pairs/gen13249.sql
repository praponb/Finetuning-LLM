with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        element
    from price_tiers
    lateral view outer explode(discounts) x as element
)
-- Grouped metrics
select
    case when element >= 60 then 'LARGE' else 'SMALL' end as element_tier,
    size(collect_set(plan)) as num_plans,
    count(*) filter (where plan = 'pro') as pro_rows,
    count(distinct id) as distinct_ids,
    size(collect_set(currency)) as num_currencies,
    size(collect_set(currency)) as num_currencies_2
from exploded
group by 1;
