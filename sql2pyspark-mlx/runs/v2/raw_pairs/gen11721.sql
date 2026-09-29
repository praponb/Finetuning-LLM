SELECT id, store_id, channel FROM orders WHERE shipping_fee <= 38 EXCEPT SELECT id, store_id, channel FROM orders WHERE discount > 0.22 ORDER BY channel
