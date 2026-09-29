SELECT status, channel, COUNT(DISTINCT shipping_fee) AS unique_channels
FROM orders
WHERE discount BETWEEN 0.11 AND 0.4
GROUP BY status, channel;
