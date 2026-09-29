SELECT status, COUNT(DISTINCT amount) unique_methods, MIN(amount) min_amount
FROM payments
WHERE order_id = 4
GROUP BY status;
