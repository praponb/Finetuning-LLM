SELECT method, COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count FROM payments GROUP BY method HAVING refunded_count >= 4 ORDER BY 2 DESC, method LIMIT 50
