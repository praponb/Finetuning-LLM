SELECT id, customer_id, device FROM sessions WHERE browser <> 'Firefox' EXCEPT SELECT id, customer_id, device FROM sessions WHERE HOUR(started_at) BETWEEN 12 AND 15 ORDER BY customer_id;
