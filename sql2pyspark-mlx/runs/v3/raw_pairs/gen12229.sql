SELECT id, channel, order_date FROM orders WHERE store_id IN (1, 2, 24) EXCEPT SELECT id, channel, order_date FROM orders WHERE amount >= 1130 ORDER BY order_date;
