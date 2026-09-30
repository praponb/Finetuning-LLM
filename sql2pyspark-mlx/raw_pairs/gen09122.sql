WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT id, tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS tier_value
)
-- Grouped metrics
SELECT
    CASE WHEN tier_value >= 78 THEN 'HIGH' ELSE 'LOW' END AS tier_value_tier,
    SUM(tier_value) AS total_tier_value,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(tier_value) AS total_tier_value_2
FROM exploded
GROUP BY CASE WHEN tier_value >= 78 THEN 'HIGH' ELSE 'LOW' END
ORDER BY total_tier_value;
