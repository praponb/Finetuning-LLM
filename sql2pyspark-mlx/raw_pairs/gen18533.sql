SELECT status, SUM(amount) total_amount, COUNT(DISTINCT amount) unique_methods
FROM payments
WHERE (method = 'card' AND amount < 830) OR order_id IN (20, 27, 37, 40)
GROUP BY status
