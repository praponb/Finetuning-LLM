SELECT *, CONCAT(status, ' ', channel) AS status_channel, FLOOR(shipping_fee) AS shipping_fee_whole
FROM orders
WHERE status = 'COMPLETED' OR order_date <= '2024-03-01'
ORDER BY shipping_fee_whole DESC;
