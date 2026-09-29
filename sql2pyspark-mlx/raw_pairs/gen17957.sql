WITH recent_order_items AS (
    SELECT *
    FROM order_items
    WHERE order_id = 19 AND product_id = 14
),
quantity_bucket_summary AS (
    SELECT CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END quantity_bucket, COUNT(DISTINCT quantity) unique_orders, COUNT(*) total_count
    FROM recent_order_items
    GROUP BY CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END
)
SELECT quantity_bucket, unique_orders, total_count
FROM quantity_bucket_summary
ORDER BY total_count;
