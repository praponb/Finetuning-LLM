SELECT method, AVG(amount) AS avg_amount, MAX(amount) - MIN(amount) AS amount_range
FROM payments
GROUP BY method;
