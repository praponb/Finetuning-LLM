SELECT id, status, shipping_fee, AVG(shipping_fee) OVER (PARTITION BY status) AS status_avg_shipping_fee FROM orders ORDER BY status;
