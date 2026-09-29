WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) li AS tier_value
)
-- Grouped metrics
SELECT
    CASE WHEN tier_value >= 62 THEN 'HIGH' ELSE 'LOW' END AS tier_value_tier,
    ROUND(SUM(tier_value) / NULLIF(COUNT(CASE WHEN plan = 'pro' THEN 1 END), 0), 2) AS tier_value_per_pro,
    COUNT(CASE WHEN currency = 'USD' THEN 1 END) AS usd_count,
    GROUPING_ID() AS gid
FROM lines
GROUP BY ROLLUP (CASE WHEN tier_value >= 62 THEN 'HIGH' ELSE 'LOW' END);
