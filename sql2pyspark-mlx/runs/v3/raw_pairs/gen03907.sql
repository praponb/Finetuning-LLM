SELECT o.id, o.channel
FROM orders o
WHERE o.channel = 'partner' AND EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id AND s.carrier = 'Flash')
