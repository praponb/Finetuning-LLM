SELECT order_id, AVG(unit_price) AS avg_unit_price, SUM(unit_price) AS total_unit_price
FROM order_items
WHERE quantity IS NOT NULL OR quantity IS NOT NULL
GROUP BY order_id;
