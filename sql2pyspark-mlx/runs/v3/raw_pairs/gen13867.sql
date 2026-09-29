SELECT MAX(paid_at) latest_paid_at
FROM payments
WHERE amount >= 790
