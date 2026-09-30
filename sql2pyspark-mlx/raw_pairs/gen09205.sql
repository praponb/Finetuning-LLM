SELECT MONTH(c.signup_date) AS signup_month, MAX(we.duration_sec) AS max_duration_sec, AVG(we.duration_sec) AS avg_duration_sec
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
RIGHT JOIN customers c ON s.customer_id = c.id
GROUP BY MONTH(c.signup_date)
ORDER BY avg_duration_sec DESC, signup_month DESC
LIMIT 50;
