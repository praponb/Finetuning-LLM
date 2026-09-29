SELECT QUARTER(customers.signup_date) AS signup_quarter, SUM(CASE WHEN sessions.browser = 'Edge' THEN 1 ELSE 0 END) AS edge_count, MIN(sessions.started_at) AS min_started_at
FROM sessions
LEFT JOIN customers ON customers.id = sessions.customer_id
WHERE sessions.browser IS NULL
GROUP BY QUARTER(customers.signup_date);
