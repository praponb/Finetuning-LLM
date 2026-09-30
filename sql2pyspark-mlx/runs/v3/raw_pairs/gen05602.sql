SELECT c.segment, DATE_TRUNC('MONTH', c.signup_date) AS signup_month, MAX(o.shipping_fee) - MIN(o.shipping_fee) AS shipping_fee_range
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.order_date BETWEEN '2023-03-01' AND '2025-01-24'
GROUP BY c.segment, DATE_TRUNC('MONTH', c.signup_date)
HAVING shipping_fee_range > 48;
