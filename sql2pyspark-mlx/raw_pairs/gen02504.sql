WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(cost * 0.85, 2) AS cost_scaled,
        LENGTH(status) AS status_length
    FROM shipments
    WHERE carrier IS NOT NULL
      AND shipped_date BETWEEN '2024-12-05' AND '2025-01-25'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', shipped_date) AS quarter_start,
    DATE_TRUNC('WEEK', shipped_date) AS week_start,
    CASE WHEN cost >= 98 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    ROUND(AVG(status_length), 2) AS avg_status_length
FROM cleaned
GROUP BY DATE_TRUNC('QUARTER', shipped_date), DATE_TRUNC('WEEK', shipped_date), CASE WHEN cost >= 98 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(DISTINCT order_id) > 1
ORDER BY avg_status_length DESC NULLS LAST
