SELECT p.id, p.paid_at FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE store_id IS NULL)
