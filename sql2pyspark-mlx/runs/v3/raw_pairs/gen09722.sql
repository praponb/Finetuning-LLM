SELECT status
FROM orders
WHERE amount > 940
EXCEPT
SELECT status
FROM payments
WHERE order_id IN (12, 15, 16, 21)
