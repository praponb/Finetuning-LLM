SELECT method, MAX(amount) - MIN(amount) AS amount_range
FROM payments
GROUP BY 1
HAVING MAX(amount) - MIN(amount) > 68
