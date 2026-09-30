SELECT status
FROM accounts
WHERE customer_id IS NOT NULL
INTERSECT
SELECT status
FROM payments
WHERE order_id = 28
