SELECT id, discount, amount, TRIM(channel) AS clean_channel, CONCAT(channel, '/', status) AS channel_status FROM orders WHERE status != 'COMPLETED' ORDER BY id
