SELECT c.country, MONTH(c.signup_date) AS signup_month, MAX(s.started_at) AS latest_started_at
FROM sessions s
INNER JOIN customers c ON c.id = s.customer_id
GROUP BY c.country, MONTH(c.signup_date)
ORDER BY signup_month DESC, country ASC
LIMIT 10
