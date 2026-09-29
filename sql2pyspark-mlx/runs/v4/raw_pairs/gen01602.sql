SELECT c.segment, DATE_TRUNC('MONTH', c.signup_date) AS signup_month, COUNT(*) AS total_count
FROM orders o
INNER JOIN stores s ON s.id = o.store_id
INNER JOIN customers c ON o.customer_id = c.id
GROUP BY c.segment, DATE_TRUNC('MONTH', c.signup_date)
ORDER BY signup_month, segment
LIMIT 5
