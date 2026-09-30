SELECT CASE WHEN unit_price >= 420 THEN 'high' WHEN unit_price >= 140 THEN 'mid' ELSE 'low' END AS unit_price_bucket, ROUND(AVG(quantity), 2) AS avg_quantity
FROM order_items
WHERE order_id = 11
GROUP BY CASE WHEN unit_price >= 420 THEN 'high' WHEN unit_price >= 140 THEN 'mid' ELSE 'low' END
