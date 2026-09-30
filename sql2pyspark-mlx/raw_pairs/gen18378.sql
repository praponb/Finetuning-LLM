WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) li AS element
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    AVG(element) AS avg_element
FROM exploded
GROUP BY 1, 2
ORDER BY avg_element ASC NULLS LAST
