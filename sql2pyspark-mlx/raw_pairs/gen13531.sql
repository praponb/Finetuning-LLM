SELECT s.id, s.cost, s.delivered_date FROM shipments s WHERE s.order_id IS NULL AND s.order_id IN (SELECT id FROM orders WHERE channel <> 'web');
