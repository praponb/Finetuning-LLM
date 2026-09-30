SELECT s.started_at, s.browser, c.is_active, DATEDIFF(CURRENT_DATE(), s.started_at) AS days_since_started_at FROM sessions s FULL OUTER JOIN customers c ON c.id = s.customer_id
