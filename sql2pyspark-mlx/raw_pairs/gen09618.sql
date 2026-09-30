with unpacked as (
    -- Higher-order array functions
    select
        id,
        effective_from,
        tier_prices,
        discounts,
        transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps,
        element_at(tier_prices, 1) as entry_price
    from price_tiers
)
-- Aggregate with conditional metrics
select
    case when entry_price >= 26 then 'PREMIUM' else 'STANDARD' end as entry_price_tier,
    date_trunc('week', effective_from) as week_start,
    max(entry_price) as max_entry_price,
    round(avg(entry_price), 2) as avg_entry_price
from unpacked
group by case when entry_price >= 26 then 'PREMIUM' else 'STANDARD' end, date_trunc('week', effective_from)
having round(avg(entry_price), 2) < 1000000
order by max_entry_price;
