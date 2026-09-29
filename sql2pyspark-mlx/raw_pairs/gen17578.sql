SELECT 'account' AS origin, status
FROM accounts
WHERE customer_id IN (7, 13, 20, 28)
UNION ALL
SELECT 'payment' AS origin, status
FROM payments;
