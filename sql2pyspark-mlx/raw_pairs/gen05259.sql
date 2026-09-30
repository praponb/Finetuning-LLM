SELECT status, 'order' AS kind
FROM orders
UNION ALL
SELECT status, 'account' AS kind
FROM accounts
