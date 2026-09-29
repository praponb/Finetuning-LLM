SELECT shipments.id, shipments.status, shipments.delivered_date FROM shipments WHERE shipments.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee <= 19) ORDER BY shipments.status, id LIMIT 50;
