SELECT YEAR(c.signup_date) AS signup_year, MAX(we.duration_sec) AS max_duration_sec
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
INNER JOIN customers c ON s.customer_id = c.id
WHERE s.country = 'DE'
GROUP BY YEAR(c.signup_date)
HAVING MAX(we.duration_sec) >= 450
ORDER BY signup_year;
