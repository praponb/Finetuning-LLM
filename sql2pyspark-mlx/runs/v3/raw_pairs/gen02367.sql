SELECT DATE_TRUNC('MONTH', products.launch_date) AS launch_month, MIN(order_items.quantity) AS min_quantity
FROM order_items
FULL OUTER JOIN orders ON order_items.order_id = orders.id
INNER JOIN products ON order_items.product_id = products.id
GROUP BY DATE_TRUNC('MONTH', products.launch_date);
