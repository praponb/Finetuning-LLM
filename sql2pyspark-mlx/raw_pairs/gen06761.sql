SELECT status, MAX(amount) - MIN(amount) AS amount_range
FROM payments
GROUP BY status
