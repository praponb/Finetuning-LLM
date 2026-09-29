SELECT c.is_active, DATE_FORMAT(c.signup_date, 'yyyy-MM') AS signup_month, SUM(CASE WHEN s.browser = 'Chrome' THEN 1 ELSE 0 END) AS chrome_count
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY c.is_active, DATE_FORMAT(c.signup_date, 'yyyy-MM');
