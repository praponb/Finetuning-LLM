SELECT store_id, shipping_fee, CONCAT(channel, '/', status) channel_status
FROM orders
WHERE (MONTH(order_date) IN (3, 9, 10) AND shipping_fee > 23) OR amount BETWEEN 1170 AND 1470;
