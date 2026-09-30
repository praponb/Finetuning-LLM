SELECT id, status, shipping_fee, MAX(shipping_fee) OVER (PARTITION BY status) status_max_shipping_fee FROM orders WHERE YEAR(order_date) = 2025
