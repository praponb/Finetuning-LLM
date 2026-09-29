SELECT status, MAX(paid_at) AS latest_paid_at, MAX(amount) AS max_amount
FROM payments
GROUP BY status;
