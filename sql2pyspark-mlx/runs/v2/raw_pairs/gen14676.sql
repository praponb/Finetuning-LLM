SELECT status, 'order' AS source
FROM orders
UNION ALL
SELECT status, 'account' AS source
FROM accounts
WHERE account_type IN ('CHECKING', 'LOAN', 'CREDIT');
