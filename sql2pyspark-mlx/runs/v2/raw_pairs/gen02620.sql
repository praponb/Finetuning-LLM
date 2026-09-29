SELECT COUNT(DISTINCT shipping_fee) AS unique_channels, ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee, SUM(amount) AS total_amount
FROM orders
WHERE order_date < '2025-08-25' AND store_id = 1;
