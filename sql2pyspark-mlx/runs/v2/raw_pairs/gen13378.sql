SELECT order_id, ROUND(AVG(unit_price), 2) AS avg_unit_price, AVG(quantity) AS avg_quantity
FROM order_items
GROUP BY order_id
