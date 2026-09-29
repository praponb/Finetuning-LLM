SELECT amount, discount, status, CONCAT_WS('/', channel, status) channel_status_key FROM orders WHERE shipping_fee BETWEEN 13 AND 40 AND store_id = 9 ORDER BY discount
