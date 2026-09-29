WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN tier_value >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_tier,
    ROUND(AVG(tier_value), 2) AS avg_tier_value,
    MAX(tier_value) FILTER (WHERE currency = 'EUR') AS eur_max_tier_value,
    MAX(tier_value) AS max_tier_value,
    SUM(CASE WHEN plan = 'pro' THEN tier_value ELSE 0 END) AS pro_tier_value,
    ROUND(AVG(tier_value), 2) AS avg_tier_value_2,
    COUNT(*) AS num_rows
FROM flattened
GROUP BY CASE WHEN tier_value >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END
