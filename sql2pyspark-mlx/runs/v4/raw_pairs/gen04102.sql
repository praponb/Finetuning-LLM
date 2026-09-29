with lines as (
    select
        id,
        effective_from,
        currency,
        explode_outer(tier_prices) as element
    from price_tiers
)
select
    id,
    effective_from,
    currency
from lines
order by effective_from nulls first;
