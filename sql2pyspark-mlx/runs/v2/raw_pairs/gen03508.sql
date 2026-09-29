SELECT id, customer_id FROM sessions WHERE HOUR(started_at) BETWEEN 12 AND 18 UNION ALL SELECT id, customer_id FROM sessions WHERE HOUR(started_at) BETWEEN 7 AND 14 ORDER BY id;
