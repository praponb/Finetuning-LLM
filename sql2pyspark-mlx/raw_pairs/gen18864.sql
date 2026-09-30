SELECT
    c.is_active,
    DATE_TRUNC('MONTH', c.signup_date) AS signup_month,
    MAX(o.shipping_fee) AS max_shipping_fee,
    COUNT(*) AS row_count
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
JOIN customers c ON o.customer_id = c.id
WHERE o.customer_id = 21
GROUP BY c.is_active, DATE_TRUNC('MONTH', c.signup_date)
