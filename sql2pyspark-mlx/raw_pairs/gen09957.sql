SELECT order_id, SUM(unit_price) AS total_unit_price, SUM(SUM(unit_price)) OVER (ORDER BY order_id) AS cumulative_unit_price
FROM order_items
WHERE order_id IN (9, 10, 13, 28)
GROUP BY order_id;
