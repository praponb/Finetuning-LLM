SELECT status
FROM accounts
UNION
SELECT status
FROM payments
WHERE TO_DATE(paid_at) < '2023-03-01'
