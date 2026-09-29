SELECT id, customer_id, store_id FROM orders WHERE shipping_fee <= 14 UNION SELECT id, customer_id, store_id FROM orders WHERE discount BETWEEN 0.35 AND 0.38 ORDER BY id;
