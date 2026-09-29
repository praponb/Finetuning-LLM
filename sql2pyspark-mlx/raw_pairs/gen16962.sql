WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        shipped_date,
        DATEDIFF(CURRENT_DATE(), delivered_date) AS days_since_delivered_date
    FROM shipments
    WHERE status NOT IN ('IN_TRANSIT', 'DELIVERED')
)
-- Multi-level aggregation
SELECT
    CASE WHEN days_since_delivered_date >= 87 THEN 'HIGH' ELSE 'LOW' END AS days_since_delivered_date_tier,
    APPROX_PERCENTILE(days_since_delivered_date, ARRAY(0.25, 0.5, 0.75)) AS days_since_delivered_date_quartiles,
    COUNT(DISTINCT id) AS distinct_ids
FROM filtered
GROUP BY CASE WHEN days_since_delivered_date >= 87 THEN 'HIGH' ELSE 'LOW' END;
