WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_limits,
        discounts,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
)
-- Aggregate with conditional metrics
SELECT
    currency,
    SUM(CASE WHEN plan = 'pro' THEN entry_price ELSE 0 END) AS pro_entry_price,
    APPROX_PERCENTILE(avg_tier_price, ARRAY(0.25, 0.5, 0.75)) AS avg_tier_price_quartiles,
    SIZE(COLLECT_SET(currency)) AS num_currencies,
    COUNT(CASE WHEN currency = 'USD' THEN 1 END) AS usd_count
FROM parsed
GROUP BY currency
