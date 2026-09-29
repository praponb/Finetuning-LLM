WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_limits) AS element
    FROM price_tiers
)
-- Aggregate with conditional metrics
SELECT
    currency,
    COUNT(*) AS num_rows,
    COUNT(CASE WHEN plan = 'pro' THEN 1 END) AS pro_count,
    SIZE(COLLECT_SET(plan)) AS num_plans
FROM lines
GROUP BY currency
HAVING SIZE(COLLECT_SET(plan)) < 1000
ORDER BY num_plans NULLS LAST;
