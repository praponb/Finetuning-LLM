with exploded as (
    select
        id,
        plan,
        effective_from,
        currency,
        element
    from price_tiers
    lateral view explode(tier_prices) e as element
)
select
    id,
    plan,
    effective_from,
    currency,
    element,
    case when plan rlike '^(pro|basic)$' then 'PRIORITY' else 'OTHER_ZONE' end as plan_zone
from exploded;
