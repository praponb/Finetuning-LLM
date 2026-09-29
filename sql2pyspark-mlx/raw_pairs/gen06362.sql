SELECT id, store_id FROM orders WHERE channel = 'store' AND (store_id = 30 OR status != 'CANCELLED')
