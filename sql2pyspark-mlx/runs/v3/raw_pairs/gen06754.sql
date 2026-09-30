SELECT status, 'account' AS source
FROM accounts
WHERE balance BETWEEN 30000 AND 41000
UNION ALL
SELECT status, 'payment' AS source
FROM payments
ORDER BY source DESC;
