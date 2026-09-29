SELECT SUM(amount) AS total_amount, COUNT(DISTINCT amount) AS unique_statuss
FROM orders
WHERE customer_id IN (9, 29)
