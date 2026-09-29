SELECT p.paid_at, o.customer_id, o.discount FROM payments p INNER JOIN orders o ON p.order_id = o.id WHERE p.order_id IN (5, 8, 25) AND o.store_id IN (7, 12, 17) ORDER BY o.discount
