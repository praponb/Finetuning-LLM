SELECT oi.id, oi.quantity, oi.unit_price, o.customer_id, o.discount, o.shipping_fee FROM order_items oi JOIN orders o ON oi.order_id = o.id ORDER BY oi.unit_price, id DESC LIMIT 5;
