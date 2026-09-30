SELECT AVG(amount) AS avg_amount, MAX(amount) AS max_amount
FROM payments
WHERE amount > 1510 OR order_id IN (2, 8)
ORDER BY max_amount
