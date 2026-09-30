SELECT id, channel, store_id FROM orders WHERE store_id IS NOT NULL UNION ALL SELECT id, channel, store_id FROM orders WHERE MONTH(order_date) IN (4, 5, 10) ORDER BY id DESC;
