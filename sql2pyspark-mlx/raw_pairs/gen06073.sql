SELECT p.category, ROUND(STDDEV(oi.unit_price), 2) stddev_unit_price FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id INNER JOIN products p ON oi.product_id = p.id GROUP BY p.category
