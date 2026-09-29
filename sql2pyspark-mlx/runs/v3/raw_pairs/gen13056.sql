SELECT c.city, DATE_FORMAT(c.signup_date, 'yyyy-MM') AS signup_month, SUM(CASE WHEN s.status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_count
FROM subscriptions s
INNER JOIN customers c ON c.id = s.customer_id
GROUP BY c.city, DATE_FORMAT(c.signup_date, 'yyyy-MM')
