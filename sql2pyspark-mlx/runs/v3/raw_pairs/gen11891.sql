SELECT status
FROM orders
WHERE customer_id = 8
UNION ALL
SELECT status
FROM accounts
WHERE status = 'CLOSED';
