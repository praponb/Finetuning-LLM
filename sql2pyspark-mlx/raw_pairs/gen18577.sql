SELECT product_id, CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 140 THEN 'mid' ELSE 'low' END AS unit_price_bucket, COUNT(DISTINCT unit_price) AS unique_orders
FROM order_items
WHERE unit_price IS NULL
GROUP BY 1, 2
