SELECT p.id, p.amount, o.customer_id FROM payments p LEFT JOIN orders o ON p.order_id = o.id WHERE p.status = 'REFUNDED';
