SELECT status FROM orders WHERE shipping_fee > 33 INTERSECT SELECT status FROM shipments;
