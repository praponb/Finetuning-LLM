SELECT status, SUM(CASE WHEN method = 'paypal' THEN 1 ELSE 0 END) AS paypal_count
FROM payments
WHERE amount > 1110 OR status = 'FAILED'
GROUP BY status
