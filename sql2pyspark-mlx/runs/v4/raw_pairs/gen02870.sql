SELECT oi.order_id, o.status, p.brand, p.category FROM order_items oi JOIN orders o ON oi.order_id = o.id LEFT JOIN products p ON oi.product_id = p.id;
