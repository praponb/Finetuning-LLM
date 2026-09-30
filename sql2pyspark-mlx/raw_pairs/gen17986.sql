with flattened_attrs as (
    -- Semi-structured parsing
    select
        id,
        tier_limits,
        tier_prices,
        zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices,
        aggregate(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
        element_at(tier_prices, 1) as entry_price,
        array_min(tier_prices) as lowest_price
    from price_tiers
)
-- Running and moving aggregates
select
    id,
    entry_price,
    lowest_price,
    count(lowest_price) over (order by entry_price, id rows between 2 preceding and current row) as last3_count_lowest_price,
    min(lowest_price) over (order by entry_price, id rows between 2 preceding and current row) as last3_min_lowest_price,
    sum(lowest_price) over (order by entry_price, id rows between 2 preceding and current row) as last3_sum_lowest_price
from flattened_attrs;
