WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) e AS val
)
-- Aggregate with conditional metrics
SELECT
    plan,
    currency,
    ROUND(SUM(val) / NULLIF(COUNT(CASE WHEN plan = 'starter' THEN 1 END), 0), 2) AS val_per_starter,
    SUM(val) AS total_val
FROM flattened
GROUP BY plan, currency
