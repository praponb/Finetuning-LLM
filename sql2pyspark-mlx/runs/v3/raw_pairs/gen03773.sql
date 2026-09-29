SELECT *, COALESCE(channel, 'unassigned') AS channel_filled, COALESCE(shipping_fee, 0) AS shipping_fee_filled FROM orders
