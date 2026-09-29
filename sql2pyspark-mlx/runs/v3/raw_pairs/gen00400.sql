SELECT products.brand, SUM(order_items.quantity * order_items.unit_price) AS revenue, ROUND(STDDEV(order_items.quantity), 2) AS stddev_quantity
FROM order_items
INNER JOIN orders ON order_items.order_id = orders.id
FULL OUTER JOIN products ON order_items.product_id = products.id
GROUP BY products.brand
HAVING ROUND(STDDEV(order_items.quantity), 2) > 67
ORDER BY revenue, products.brand DESC
LIMIT 10;
