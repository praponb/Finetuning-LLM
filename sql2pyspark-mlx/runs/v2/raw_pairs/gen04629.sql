SELECT status, amount, SUBSTRING(channel, 1, 2) AS channel_prefix, COALESCE(discount, 0) AS discount_filled FROM orders
