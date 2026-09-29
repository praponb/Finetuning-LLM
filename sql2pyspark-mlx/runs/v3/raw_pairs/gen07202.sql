SELECT p.id, p.amount, p.method, p.order_id, o.discount, o.store_id FROM payments p INNER JOIN orders o ON p.order_id = o.id
