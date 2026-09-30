SELECT *, CONCAT_WS(',', channel, status) AS channel_status_key
FROM orders
WHERE store_id IN (11, 15, 32, 40) AND store_id = 8
ORDER BY channel_status_key
