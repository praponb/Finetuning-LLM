WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        plan,
        effective_from,
        tier_prices,
        discounts,
        ARRAY_MIN(tier_prices) AS lowest_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
    FROM price_tiers
    WHERE YEAR(effective_from) = 2025
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    SUM(CASE WHEN plan = 'starter' THEN lowest_price ELSE 0 END) AS starter_lowest_price,
    ROUND(AVG(lowest_price), 2) AS avg_lowest_price,
    COUNT(DISTINCT plan) AS distinct_plans
FROM flattened_attrs
GROUP BY 1
ORDER BY avg_lowest_price DESC
