WITH base AS (
    SELECT *
    FROM inventory
    WHERE product_id IN (1, 5, 8, 9)
),
agg AS (
    SELECT
        CASE WHEN quantity >= 246 THEN 'high' WHEN quantity >= 98 THEN 'mid' ELSE 'low' END AS quantity_bucket,
        MONTH(last_restock) AS last_restock_month,
        COUNT(DISTINCT quantity) AS unique_products,
        ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM base
    GROUP BY CASE WHEN quantity >= 246 THEN 'high' WHEN quantity >= 98 THEN 'mid' ELSE 'low' END, MONTH(last_restock)
)
SELECT *
FROM agg
WHERE unique_products >= 9
ORDER BY last_restock_month, quantity_bucket DESC
LIMIT 10
