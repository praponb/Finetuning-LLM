SELECT DATE_FORMAT(c.signup_date, 'yyyy-MM') AS signup_month, COUNT(DISTINCT s.country) AS unique_countrys
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE s.customer_id = 8
GROUP BY DATE_FORMAT(c.signup_date, 'yyyy-MM');
