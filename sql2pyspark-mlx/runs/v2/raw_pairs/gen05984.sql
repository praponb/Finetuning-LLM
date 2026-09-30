SELECT method, COUNT(amount) AS amount_count
FROM payments
GROUP BY 1
HAVING SUM(amount) >= 3600
ORDER BY amount_count DESC, method DESC
