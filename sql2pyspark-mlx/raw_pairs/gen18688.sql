SELECT method, SUM(amount) total_amount, DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
FROM payments
WHERE paid_at >= '2024-07-01'
GROUP BY method
ORDER BY rnk ASC
