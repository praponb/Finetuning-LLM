SELECT s.order_id, s.shipped_date, s.carrier, o.discount, o.status, o.order_date FROM shipments s LEFT JOIN orders o ON s.order_id = o.id WHERE s.carrier = 'Kerry' AND o.store_id IS NULL
