with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        explode_outer(tier_prices) as tier_value
    from price_tiers
)
select
    id,
    effective_from,
    currency
from exploded
order by effective_from desc, id desc
