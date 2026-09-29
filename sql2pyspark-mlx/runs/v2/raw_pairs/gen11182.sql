SELECT order_id, CASE WHEN unit_price >= 350 THEN 'high' WHEN unit_price >= 160 THEN 'mid' ELSE 'low' END AS unit_price_bucket, ROUND(AVG(quantity), 2) AS avg_quantity
FROM order_items
WHERE quantity != 4 AND unit_price <= 240 AND order_id = 34
GROUP BY order_id, CASE WHEN unit_price >= 350 THEN 'high' WHEN unit_price >= 160 THEN 'mid' ELSE 'low' END
