SELECT id, country, DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at
FROM sessions
WHERE customer_id = 16 AND (customer_id = 15 OR browser IS NOT NULL);
