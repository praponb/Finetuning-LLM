SELECT status
FROM orders
EXCEPT
SELECT status
FROM accounts
ORDER BY status DESC;
