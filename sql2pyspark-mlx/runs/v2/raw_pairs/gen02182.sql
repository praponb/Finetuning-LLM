SELECT * FROM (SELECT status, COUNT(CASE WHEN status = 'PAUSED' THEN 1 END) paused_count FROM subscriptions GROUP BY status) s WHERE paused_count >= 3
