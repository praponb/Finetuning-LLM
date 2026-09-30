SELECT id, channel, discount, SUM(discount) OVER (PARTITION BY channel) AS channel_sum_discount FROM orders
