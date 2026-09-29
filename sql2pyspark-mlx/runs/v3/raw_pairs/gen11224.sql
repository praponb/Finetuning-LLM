SELECT status, SUM(CASE WHEN method = 'paypal' THEN 1 ELSE 0 END) paypal_count
FROM payments
GROUP BY status
HAVING COUNT(DISTINCT amount) >= 3
ORDER BY status DESC, 2
LIMIT 3;
