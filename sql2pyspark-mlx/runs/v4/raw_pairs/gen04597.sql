with unpacked as (
    select
        id,
        plan,
        effective_from,
        currency,
        tier_limits,
        discounts,
        aggregate(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
        array_min(tier_prices) as lowest_price,
        element_at(tier_prices, 1) as entry_price
    from price_tiers
)
select
    id,
    plan,
    effective_from,
    currency,
    tier_limits,
    discounts,
    avg_tier_price,
    lowest_price,
    entry_price,
    sum(entry_price) over w_recent as running_entry_price,
    lead(avg_tier_price, 1) over w_ordered as next_avg_tier_price
from unpacked
window w_recent as (partition by plan order by lowest_price, id rows between unbounded preceding and current row), w_ordered as (order by avg_tier_price, id);
