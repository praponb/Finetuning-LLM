SELECT *, ROUND(shipping_fee * 0.9, 2) shipping_fee_adjusted, DATE_TRUNC('MONTH', order_date) order_date_period
FROM orders
WHERE customer_id = 22 AND (status IN ('SHIPPED', 'CANCELLED', 'RETURNED') OR order_date > '2025-04-01')
ORDER BY shipping_fee DESC;
