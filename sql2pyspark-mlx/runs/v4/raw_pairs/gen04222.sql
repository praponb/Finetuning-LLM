with flattened_attrs as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        plan,
        effective_from,
        tier_prices,
        array_min(tier_prices) as lowest_price,
        zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices,
        element_at(tier_prices, 1) as entry_price,
        aggregate(tier_prices, 0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
        transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps
    from price_tiers
)
-- Ranking and gap features
select
    *,
    count(avg_tier_price) over w_ordered as plan_count_avg_tier_price,
    stddev_samp(entry_price) over w_ordered as plan_stddev_samp_entry_price,
    avg(avg_tier_price) over (partition by plan order by entry_price, id rows between 5 preceding and current row) as avg_tier_price_avg_5,
    row_number() over (partition by plan order by avg_tier_price desc, id) as seq_no
from flattened_attrs
window w_ordered as (partition by plan);
