SELECT shipments.id, shipments.carrier, shipments.shipped_date FROM shipments WHERE shipments.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee BETWEEN 28 AND 29);
