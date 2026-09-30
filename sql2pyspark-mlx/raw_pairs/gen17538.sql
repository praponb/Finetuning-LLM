with exploded as (
    select
        id,
        plan,
        effective_from,
        tier_value
    from price_tiers
    lateral view outer explode(discounts) li as tier_value
)
select
    id,
    plan,
    effective_from
from exploded
order by id nulls last
