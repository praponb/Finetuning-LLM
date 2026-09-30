WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC NULLS LAST, id) AS rn
    FROM inventory
    QUALIFY rn = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN quantity >= 246 THEN 'HIGH' ELSE 'LOW' END AS quantity_tier,
    product_id,
    APPROX_PERCENTILE(quantity, 0.95) AS quantity_p95,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    MAX(quantity) AS max_quantity,
    PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    COUNT(*) AS row_count,
    GROUPING_ID() AS grouping_id
FROM ranked
GROUP BY ROLLUP (CASE WHEN quantity >= 246 THEN 'HIGH' ELSE 'LOW' END, product_id);
