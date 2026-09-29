SELECT id, country FROM sessions WHERE NOT ((started_at < '2024-01-01' AND HOUR(started_at) BETWEEN 12 AND 16) OR customer_id IS NULL);
