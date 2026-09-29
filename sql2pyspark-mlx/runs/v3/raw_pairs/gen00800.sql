SELECT oi.quantity, oi.product_id, p.cost, o.customer_id FROM order_items oi LEFT OUTER JOIN products p ON p.id = oi.product_id LEFT OUTER JOIN orders o ON o.id = oi.order_id ORDER BY customer_id;
