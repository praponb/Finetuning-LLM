SELECT status, COALESCE(channel, 'N/A') AS channel_filled
FROM orders
WHERE store_id IN (3, 34);
