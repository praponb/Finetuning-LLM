SELECT id, shipping_fee, DATE_TRUNC('year', order_date) AS order_date_period FROM orders WHERE status IN ('COMPLETED', 'SHIPPED') OR order_date <= '2024-08-03';
