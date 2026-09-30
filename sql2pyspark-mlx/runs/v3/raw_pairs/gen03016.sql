SELECT id, shipping_fee, COALESCE(channel, 'Unknown') AS channel_filled
FROM orders
WHERE status = 'COMPLETED' AND store_id IN (30, 33, 37);
