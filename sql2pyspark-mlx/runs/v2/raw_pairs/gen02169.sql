SELECT status, MIN(paid_at) AS min_paid_at, COUNT(amount) AS amount_count
FROM payments
GROUP BY status
HAVING COUNT(amount) < 1;
