with flattened as (
    select
        id,
        effective_from,
        explode_outer(tier_limits) as element
    from price_tiers
)
select effective_from
from flattened
order by effective_from
