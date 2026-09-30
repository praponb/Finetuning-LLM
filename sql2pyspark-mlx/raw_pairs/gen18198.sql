SELECT id, store_id, order_date FROM orders WHERE shipping_fee IS NOT NULL EXCEPT SELECT id, store_id, order_date FROM orders WHERE store_id IS NOT NULL;
