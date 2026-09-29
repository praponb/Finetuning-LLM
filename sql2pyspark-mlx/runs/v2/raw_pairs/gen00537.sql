SELECT o.customer_id, MIN(oi.unit_price) AS min_unit_price FROM order_items oi INNER JOIN products p ON p.id = oi.product_id LEFT JOIN orders o ON oi.order_id = o.id GROUP BY o.customer_id;
