SELECT id, shipping_fee, CONCAT_WS(',', channel, status) channel_status_key
FROM orders
WHERE NOT (shipping_fee >= 16 AND discount > 0.13)
