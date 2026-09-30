with exploded as (
    select
        id,
        plan,
        effective_from,
        currency,
        element
    from price_tiers
    lateral view outer explode(discounts) e as element
)
select
    id,
    effective_from,
    currency,
    element
from exploded
order by id;
