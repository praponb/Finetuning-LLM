SELECT c.segment, MAX(s.started_at) latest_started_at FROM sessions s LEFT OUTER JOIN customers c ON s.customer_id = c.id WHERE c.signup_date <= '2023-05-01' GROUP BY c.segment;
