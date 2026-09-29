SELECT status
FROM orders
WHERE channel IN ('mobile', 'web', 'partner')
INTERSECT
SELECT status
FROM accounts
