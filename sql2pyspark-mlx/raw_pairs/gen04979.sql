SELECT id, discount, TRIM(status) AS clean_status FROM orders WHERE store_id = 12 AND channel = 'web';
