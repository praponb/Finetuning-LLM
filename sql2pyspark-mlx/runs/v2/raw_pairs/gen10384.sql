SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END quantity_bucket, MIN(quantity) min_quantity, SUM(unit_price * quantity) revenue
FROM order_items
WHERE NOT (unit_price < 310 AND unit_price >= 230 AND order_id = 24)
GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
