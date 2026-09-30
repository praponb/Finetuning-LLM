SELECT id, status FROM orders WHERE shipping_fee >= 29 UNION ALL SELECT id, status FROM orders WHERE store_id IS NOT NULL;
