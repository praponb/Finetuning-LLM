SELECT QUARTER(c.signup_date) AS signup_quarter, MAX(s.started_at) AS latest_started_at, COUNT(DISTINCT s.country) AS unique_countrys
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY QUARTER(c.signup_date)
ORDER BY unique_countrys DESC, signup_quarter ASC
LIMIT 5;
