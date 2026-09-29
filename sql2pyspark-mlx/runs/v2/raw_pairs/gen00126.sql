SELECT status, SUM(amount) AS total_amount
FROM orders
WHERE NOT (order_date >= '2024-05-01' AND discount >= 0.11)
GROUP BY 1
ORDER BY status, 2
