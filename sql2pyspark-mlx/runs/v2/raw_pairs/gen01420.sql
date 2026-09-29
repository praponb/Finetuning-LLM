SELECT s.id, s.shipped_date, s.cost, o.channel, o.status, o.store_id FROM shipments s JOIN orders o ON o.id = s.order_id
