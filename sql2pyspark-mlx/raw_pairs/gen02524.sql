WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN units >= 34 THEN 'High' WHEN units >= 11 THEN 'Medium' ELSE 'Low' END AS units_tier,
        CEIL(units) AS units_whole
    FROM store_sales
    WHERE product_id IN (1, 3, 9, 10)
)
-- Join a dimension and aggregate
SELECT
    p.category,
    COUNT(*) AS num_rows,
    ROUND(SUM(s.units_whole), 2) AS total_units_whole,
    SUM(s.revenue) AS total_revenue,
    GROUPING_ID() AS gid
FROM base s
INNER JOIN products p ON s.product_id = p.id
GROUP BY ROLLUP (p.category)
ORDER BY total_units_whole;
