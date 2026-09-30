WITH parsed AS (
    SELECT
        id,
        tier_limits,
        tier_prices,
        discounts,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
    FROM price_tiers
)
SELECT
    CASE WHEN entry_price >= 29 THEN 'PREMIUM' ELSE 'STANDARD' END AS entry_price_tier,
    PERCENTILE_APPROX(entry_price, ARRAY(0.25, 0.5, 0.75)) AS entry_price_quartiles,
    COUNT(DISTINCT id) AS distinct_ids
FROM parsed
GROUP BY 1
