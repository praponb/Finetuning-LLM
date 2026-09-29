SELECT customer_id, country FROM sessions WHERE HOUR(started_at) BETWEEN 9 AND 12 UNION ALL SELECT customer_id, country FROM sessions WHERE customer_id IS NOT NULL ORDER BY country;
