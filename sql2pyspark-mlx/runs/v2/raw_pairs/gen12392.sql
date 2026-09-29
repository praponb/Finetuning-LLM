SELECT status
FROM accounts
UNION ALL
SELECT status
FROM payments
WHERE method = 'bank_transfer'
ORDER BY status;
