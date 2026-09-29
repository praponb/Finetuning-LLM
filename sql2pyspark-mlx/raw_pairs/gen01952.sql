with unpacked as (
    -- Semi-structured parsing
    select
        id,
        effective_from,
        currency,
        tier_prices,
        zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices,
        slice(tier_limits, 1, 2) as first_two_limits
    from price_tiers
    where currency = 'USD'
)
-- Aggregate with conditional metrics
select
    currency,
    size(collect_set(currency)) as num_currencies,
    size(collect_set(currency)) as num_currencies_2
from unpacked
group by currency
order by num_currencies_2 desc;
