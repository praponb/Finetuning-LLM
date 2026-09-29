SELECT o.id, o.customer_id, o.shipping_fee FROM orders o WHERE o.id IN (SELECT order_id FROM payments WHERE HOUR(paid_at) BETWEEN 8 AND 11);
