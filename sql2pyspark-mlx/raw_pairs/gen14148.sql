SELECT s.id, s.carrier, o.discount FROM shipments s JOIN orders o ON s.order_id = o.id WHERE o.status = 'PENDING'
