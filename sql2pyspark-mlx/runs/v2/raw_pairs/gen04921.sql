SELECT status, COUNT(*) AS payment_count FROM payments WHERE HOUR(paid_at) BETWEEN 6 AND 12 GROUP BY status HAVING payment_count > 3 ORDER BY payment_count, status LIMIT 3
