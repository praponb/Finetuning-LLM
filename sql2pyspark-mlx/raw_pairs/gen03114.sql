SELECT DATE_TRUNC('MONTH', c.signup_date) AS signup_month, COUNT(*) AS total_count, COUNT(*) AS cnt
FROM sessions s
LEFT JOIN customers c ON c.id = s.customer_id
GROUP BY DATE_TRUNC('MONTH', c.signup_date);
