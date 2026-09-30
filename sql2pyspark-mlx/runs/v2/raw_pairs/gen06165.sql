SELECT status, MAX(paid_at) latest_paid_at
FROM payments
WHERE amount BETWEEN 720 AND 1300 AND method <> 'card'
GROUP BY status;
