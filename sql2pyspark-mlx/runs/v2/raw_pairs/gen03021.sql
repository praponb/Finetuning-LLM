SELECT o.id, o.order_date FROM orders o WHERE EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id)
