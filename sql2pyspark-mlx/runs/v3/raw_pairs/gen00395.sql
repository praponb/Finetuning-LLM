SELECT order_id, AVG(unit_price) AS avg_unit_price, MAX(quantity) AS max_quantity
FROM order_items
WHERE order_id IN (11, 33, 36, 38) AND product_id IS NOT NULL
GROUP BY order_id;
