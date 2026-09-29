SELECT oi.unit_price, o.customer_id, o.discount, p.brand, p.product_name FROM order_items oi JOIN orders o ON oi.order_id = o.id JOIN products p ON oi.product_id = p.id;
