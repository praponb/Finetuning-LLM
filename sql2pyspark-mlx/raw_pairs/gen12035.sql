SELECT order_id, SUM(unit_price) AS total_unit_price, SUM(SUM(unit_price)) OVER (ORDER BY order_id) AS cumulative_unit_price
FROM order_items
GROUP BY order_id
ORDER BY cumulative_unit_price DESC, order_id
LIMIT 50
