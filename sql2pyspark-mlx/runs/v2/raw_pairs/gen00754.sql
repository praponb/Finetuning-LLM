SELECT status, 'account' AS source
FROM accounts
UNION
SELECT status, 'payment' AS source
FROM payments;
