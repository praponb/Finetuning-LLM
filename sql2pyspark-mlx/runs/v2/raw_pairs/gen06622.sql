SELECT status
FROM orders
WHERE discount BETWEEN 0.29 AND 0.31
UNION ALL
SELECT status
FROM accounts
ORDER BY status;
