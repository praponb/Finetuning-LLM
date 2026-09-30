WITH flattened_attrs AS (
    SELECT
        id,
        effective_from,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ARRAY_MIN(tier_prices) AS lowest_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
    WHERE effective_from BETWEEN '2024-02-01' AND '2024-03-26'
)
SELECT
    CASE WHEN lowest_price >= 26 THEN 'LARGE' ELSE 'SMALL' END AS lowest_price_tier,
    MAX(lowest_price) AS max_lowest_price,
    COUNT(*) AS event_count,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(lowest_price, 0.5) AS lowest_price_p50,
    PERCENTILE_APPROX(entry_price, 0.95) AS entry_price_p95
FROM flattened_attrs
GROUP BY lowest_price_tier
