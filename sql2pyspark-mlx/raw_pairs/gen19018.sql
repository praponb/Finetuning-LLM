SELECT order_id, CASE WHEN unit_price >= 380 THEN 'high' WHEN unit_price >= 70 THEN 'mid' ELSE 'low' END AS unit_price_bucket, AVG(quantity) AS avg_quantity
FROM order_items
WHERE order_id = 10 AND unit_price > 200 AND quantity = 7
GROUP BY order_id, unit_price_bucket
ORDER BY avg_quantity DESC, unit_price_bucket DESC;
