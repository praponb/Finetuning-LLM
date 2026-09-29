SELECT id, country, device FROM sessions WHERE customer_id IS NOT NULL UNION ALL SELECT id, country, device FROM sessions WHERE HOUR(started_at) BETWEEN 11 AND 19 ORDER BY country;
