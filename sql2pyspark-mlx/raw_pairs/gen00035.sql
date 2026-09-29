SELECT p.id, p.order_id, p.status FROM payments p WHERE p.method = 'paypal' AND p.order_id IN (SELECT id FROM orders WHERE discount > 0.11) ORDER BY status;
