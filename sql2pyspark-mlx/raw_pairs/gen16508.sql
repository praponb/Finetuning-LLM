SELECT id, device, customer_id, CASE WHEN browser = 'Chrome' THEN 1 ELSE 0 END AS is_chrome, DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at FROM sessions;
