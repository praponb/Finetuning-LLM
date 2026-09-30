SELECT p.id, p.paid_at, p.method, o.store_id FROM payments p LEFT OUTER JOIN orders o ON p.order_id = o.id;
