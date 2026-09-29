SELECT s.id, s.delivered_date, s.carrier, s.cost FROM shipments s WHERE s.status = 'LOST' AND s.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee < 13) ORDER BY s.carrier, id DESC;
