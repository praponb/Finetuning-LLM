SELECT status, channel, COUNT(shipping_fee) AS shipping_fee_count
FROM orders
GROUP BY status, channel
ORDER BY channel
