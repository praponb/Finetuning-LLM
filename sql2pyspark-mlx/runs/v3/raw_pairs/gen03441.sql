SELECT order_id, CASE WHEN unit_price >= 380 THEN 'High' WHEN unit_price >= 110 THEN 'Medium' ELSE 'Low' END AS unit_price_level
FROM order_items
WHERE product_id IS NULL AND order_id IN (6, 31, 32, 38);
