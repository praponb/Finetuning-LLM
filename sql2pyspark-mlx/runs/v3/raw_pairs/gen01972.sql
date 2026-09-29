SELECT id, status, store_id FROM orders WHERE shipping_fee <= 39 UNION SELECT id, status, store_id FROM orders WHERE store_id = 28 ORDER BY id;
