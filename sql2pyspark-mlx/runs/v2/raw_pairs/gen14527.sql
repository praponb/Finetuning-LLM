SELECT status, SUM(CASE WHEN status = 'RETURNED' THEN 1 ELSE 0 END) AS returned_count FROM orders GROUP BY status
