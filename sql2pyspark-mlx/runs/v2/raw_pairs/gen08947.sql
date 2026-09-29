SELECT MAX(amount) - MIN(amount) AS amount_range
FROM orders
WHERE shipping_fee <= 40;
