SELECT p.id, p.method, o.discount, o.store_id FROM payments p LEFT OUTER JOIN orders o ON p.order_id = o.id ORDER BY discount ASC, o.store_id ASC
