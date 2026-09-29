SELECT shipping_fee, COALESCE(channel, 'N/A') AS channel_filled FROM orders WHERE shipping_fee < 10 ORDER BY shipping_fee DESC;
