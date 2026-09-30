SELECT channel, COUNT(shipping_fee) AS shipping_fee_count
FROM orders
GROUP BY channel;
