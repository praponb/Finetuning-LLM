WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        effective_from,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ARRAY_MIN(tier_prices) AS lowest_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
    WHERE effective_from >= '2023-06-01'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN avg_tier_price >= 33 THEN 'PREMIUM' ELSE 'STANDARD' END AS avg_tier_price_tier,
    ROUND(AVG(avg_tier_price), 2) AS avg_avg_tier_price,
    MIN(lowest_price) AS min_lowest_price,
    PERCENTILE_APPROX(entry_price, 0.95) AS entry_price_p95
FROM unpacked
GROUP BY 1
ORDER BY avg_avg_tier_price DESC
