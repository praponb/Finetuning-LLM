SELECT p.order_id, o.customer_id, o.store_id FROM payments p LEFT JOIN orders o ON p.order_id = o.id WHERE p.status = 'REFUNDED';
