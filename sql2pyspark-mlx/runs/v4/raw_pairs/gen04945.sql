SELECT status
FROM accounts
EXCEPT
SELECT status
FROM payments
WHERE order_id IN (6, 11, 16, 29)
ORDER BY status DESC;
