SELECT status, 'order' AS source
FROM orders
WHERE amount >= 860
UNION
SELECT status, 'account' AS source
FROM accounts
ORDER BY status
