SELECT method, COUNT(amount) AS amount_count
FROM payments
WHERE status IN ('SUCCESS', 'FAILED') AND method = 'promptpay'
GROUP BY method
HAVING COUNT(amount) < 5
ORDER BY amount_count DESC, method
LIMIT 10
