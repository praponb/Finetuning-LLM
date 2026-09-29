with exploded as (
    select
        id,
        effective_from,
        explode(tier_limits) as element
    from price_tiers
)
select
    id,
    effective_from,
    rank() over w_hist as element_rank
from exploded
window w_hist as (order by element nulls last);
