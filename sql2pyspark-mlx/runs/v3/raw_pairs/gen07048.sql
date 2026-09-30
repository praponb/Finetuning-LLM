SELECT id, order_id, delivered_date FROM shipments WHERE carrier IN ('DHL', 'Kerry', 'UPS') INTERSECT SELECT id, order_id, delivered_date FROM shipments WHERE cost <= 70 ORDER BY order_id;
