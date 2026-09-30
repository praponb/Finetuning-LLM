SELECT YEAR(c.signup_date) AS signup_year, MAX(s.started_at) AS last_started_at
FROM sessions s
RIGHT JOIN customers c ON s.customer_id = c.id
WHERE c.age < 51
GROUP BY YEAR(c.signup_date)
ORDER BY last_started_at;
