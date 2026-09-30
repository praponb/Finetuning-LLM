SELECT status, 'account' AS source
FROM accounts
WHERE customer_id IS NULL
UNION ALL
SELECT status, 'payment' AS source
FROM payments
ORDER BY source
