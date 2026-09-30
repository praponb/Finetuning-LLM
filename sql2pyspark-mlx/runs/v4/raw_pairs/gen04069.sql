WITH flattened_attrs AS (
    SELECT
        id,
        tier_limits,
        tier_prices,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
)
SELECT
    CASE WHEN entry_price >= 28 THEN 'PREMIUM' ELSE 'STANDARD' END AS entry_price_tier,
    PERCENTILE_APPROX(entry_price, ARRAY(0.25, 0.5, 0.75)) AS entry_price_quartiles,
    APPROX_PERCENTILE(entry_price, 0.5) AS entry_price_p50,
    COUNT(*) AS row_count,
    ROUND(AVG(entry_price), 2) AS avg_entry_price,
    MAX(entry_price) AS max_entry_price,
    ROUND(AVG(entry_price), 2) AS avg_entry_price_2
FROM flattened_attrs
GROUP BY entry_price_tier
HAVING MAX(entry_price) > 0
