SELECT *, CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END AS is_completed
FROM orders
WHERE customer_id IS NULL;
