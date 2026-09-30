WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) t AS tier_value
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    plan,
    SUM(tier_value) AS total_tier_value,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE currency = 'THB') AS thb_rows,
    AVG(tier_value) AS avg_tier_value,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    GROUPING(DATE_TRUNC('QUARTER', effective_from)) AS grouping_id
FROM flattened
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', effective_from), plan), (DATE_TRUNC('QUARTER', effective_from)))
ORDER BY event_count;
