SELECT method, order_id
FROM payments
EXCEPT
SELECT method, order_id
FROM payments
WHERE status <> 'FAILED';
