SELECT status, 'account' AS source
FROM accounts
WHERE MONTH(opened_date) IN (6, 8, 11)
UNION ALL
SELECT status, 'payment' AS source
FROM payments
ORDER BY status DESC
