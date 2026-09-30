SELECT p.id, p.status, p.paid_at, p.order_id FROM payments p WHERE p.order_id NOT IN (SELECT id FROM orders WHERE store_id IN (2, 9, 24, 38));
