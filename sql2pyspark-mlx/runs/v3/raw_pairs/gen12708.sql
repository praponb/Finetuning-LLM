SELECT status
FROM accounts
WHERE account_type = 'SAVINGS'
UNION ALL
SELECT status
FROM shipments
