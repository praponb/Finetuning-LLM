SELECT id, shipping_fee, store_id FROM orders WHERE shipping_fee = (SELECT MIN(shipping_fee) FROM orders)
