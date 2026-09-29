WITH flattened_attrs AS (
    SELECT
        id,
        currency,
        tier_prices,
        ARRAY_MIN(tier_prices) AS lowest_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
SELECT
    currency,
    ROUND(AVG(lowest_price), 2) AS avg_lowest_price,
    APPROX_PERCENTILE(lowest_price, 0.95) AS lowest_price_p95,
    COUNT(CASE WHEN currency = 'THB' THEN 1 END) AS thb_count
FROM flattened_attrs
GROUP BY currency
HAVING lowest_price_p95 >= 0;
