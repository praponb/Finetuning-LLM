SELECT method, MAX(amount) - MIN(amount) AS amount_range
FROM payments
GROUP BY 1
HAVING amount_range >= 72;
