WITH base AS (
    SELECT *
    FROM inventory
    WHERE quantity = 121
),
inventory_stats AS (
    SELECT product_id, CASE WHEN quantity >= 196 THEN 'high' WHEN quantity >= 36 THEN 'mid' ELSE 'low' END quantity_bucket, ROUND(STDDEV(quantity), 2) stddev_quantity
    FROM base
    GROUP BY product_id, CASE WHEN quantity >= 196 THEN 'high' WHEN quantity >= 36 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM inventory_stats
WHERE stddev_quantity < 27
ORDER BY quantity_bucket;
