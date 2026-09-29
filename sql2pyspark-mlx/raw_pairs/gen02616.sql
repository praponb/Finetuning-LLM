WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(cost, 0) AS cost_filled
    FROM shipments
    WHERE order_id IS NOT NULL
)
-- Multi-level aggregation
SELECT
    carrier,
    CASE WHEN cost_filled >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_filled_tier,
    CASE WHEN cost >= 94 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    MIN(cost) AS min_cost,
    COUNT(*) FILTER (WHERE status = 'IN_TRANSIT') AS in_transit_rows,
    ROUND(AVG(cost), 2) AS avg_cost
FROM valid
GROUP BY carrier, CASE WHEN cost_filled >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN cost >= 94 THEN 'PREMIUM' ELSE 'STANDARD' END
