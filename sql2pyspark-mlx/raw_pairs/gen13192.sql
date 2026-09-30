SELECT status, SUM(shipping_fee) AS total_shipping_fee, SUM(SUM(shipping_fee)) OVER (ORDER BY status) AS cumulative_shipping_fee
FROM orders
GROUP BY status
ORDER BY total_shipping_fee, status DESC;
