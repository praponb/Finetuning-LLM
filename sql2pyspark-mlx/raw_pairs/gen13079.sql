WITH matched_keys AS (
    SELECT id
    FROM order_items
    INTERSECT
    SELECT id
    FROM order_items
    WHERE order_id IN (20, 29)
),
verified AS (
    SELECT s.*
    FROM order_items s
    JOIN matched_keys k ON s.id = k.id
)
SELECT
    CASE WHEN quantity >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    order_id,
    STDDEV_SAMP(unit_price) AS unit_price_stddev,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    GROUPING_ID() AS gid
FROM verified
GROUP BY GROUPING SETS ((CASE WHEN quantity >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, order_id), (CASE WHEN quantity >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY order_id DESC NULLS FIRST;
