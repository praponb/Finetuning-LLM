with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        explode_outer(tier_prices) as tier_value
    from price_tiers
)
-- Running and moving aggregates
select
    id,
    effective_from,
    dense_rank() over w_ordered as tier_value_dense_rank,
    dense_rank() over w_part as tier_value_dense_rank_2
from flattened
window w_ordered as (order by tier_value nulls first), w_part as (order by tier_value desc);
