SELECT SUM(shipping_fee) AS total_shipping_fee FROM orders WHERE status IN ('PENDING', 'CANCELLED', 'COMPLETED') ORDER BY total_shipping_fee DESC;
