SELECT oi.id, oi.product_id, o.channel, o.customer_id, p.stock_qty FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id LEFT JOIN products p ON oi.product_id = p.id;
