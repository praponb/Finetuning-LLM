SELECT MAX(amount) - MIN(amount) AS amount_range
FROM payments
ORDER BY amount_range DESC;
