SELECT c.segment, DATE_TRUNC('MONTH', c.signup_date) AS signup_month, COUNT(*) AS total_count
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
LEFT JOIN customers c ON c.id = s.customer_id
GROUP BY c.segment, DATE_TRUNC('MONTH', c.signup_date)
HAVING COUNT(*) > 4
