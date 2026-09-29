SELECT status, method, MAX(paid_at) AS last_paid_at, COUNT(*) AS num_payments FROM payments GROUP BY status, method HAVING COUNT(*) > 4 ORDER BY 4 DESC, status, method LIMIT 10;
