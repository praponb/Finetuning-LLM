SELECT SUM(shipping_fee) AS total_shipping_fee
FROM orders
ORDER BY total_shipping_fee DESC
