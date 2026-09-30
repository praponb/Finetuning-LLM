WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS tier_value
)
SELECT
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    ROUND(SUM(tier_value) / NULLIF(COUNT(CASE WHEN currency = 'USD' THEN 1 END), 0), 2) AS tier_value_per_usd,
    COUNT(DISTINCT plan) AS distinct_plans,
    MIN(tier_value) AS min_tier_value
FROM flattened
GROUP BY CUBE (DATE_TRUNC('YEAR', effective_from), DATE_TRUNC('MONTH', effective_from))
HAVING COUNT(DISTINCT plan) >= 1
ORDER BY tier_value_per_usd DESC;
