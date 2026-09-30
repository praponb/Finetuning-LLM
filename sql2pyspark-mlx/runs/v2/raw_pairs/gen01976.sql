SELECT
    product_id,
    CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 130 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    AVG(quantity) AS avg_quantity,
    COUNT(DISTINCT unit_price) AS unique_orders
FROM order_items
WHERE quantity BETWEEN 4 AND 6
GROUP BY product_id, CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END, CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 130 THEN 'mid' ELSE 'low' END
ORDER BY product_id, quantity_bucket, unit_price_bucket
LIMIT 1;
