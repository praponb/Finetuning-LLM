WITH latest AS (
    SELECT *
    FROM store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY units, id) = 1
)
SELECT
    CASE WHEN units >= 42 THEN 'PREMIUM' ELSE 'STANDARD' END AS units_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(units, 0.95) AS units_p95,
    COUNT(*) AS row_count
FROM latest
GROUP BY 1
HAVING distinct_ids >= 1
