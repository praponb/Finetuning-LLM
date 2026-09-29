SELECT products.supplier_id, MAX(order_items.unit_price) - MIN(order_items.unit_price) AS unit_price_range, SUM(order_items.quantity * order_items.unit_price) AS revenue
FROM order_items
FULL OUTER JOIN products ON order_items.product_id = products.id
RIGHT JOIN orders ON order_items.order_id = orders.id
GROUP BY products.supplier_id
HAVING unit_price_range >= 64
ORDER BY supplier_id
