SELECT order_id, ROUND(AVG(unit_price), 2) AS avg_unit_price, MAX(unit_price) - MIN(unit_price) AS unit_price_range
FROM order_items
WHERE product_id = 20
GROUP BY order_id
