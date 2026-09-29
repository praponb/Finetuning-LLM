with unpacked as (
    -- Higher-order array functions
    select
        id,
        plan,
        discounts,
        aggregate(tier_prices, 0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
        element_at(tier_prices, 1) as entry_price,
        slice(tier_limits, 1, 2) as first_two_limits,
        array_min(tier_prices) as lowest_price
    from price_tiers
    where plan = 'pro'
)
-- Running and moving aggregates
select
    id,
    avg_tier_price,
    entry_price,
    lead(lowest_price, 2) over (partition by plan order by avg_tier_price, id) as next_lowest_price,
    first_value(entry_price, true) over (order by avg_tier_price, id rows between unbounded preceding and current row) as first_known_entry_price
from unpacked;
