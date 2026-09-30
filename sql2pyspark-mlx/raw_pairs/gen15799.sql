SELECT method, SUM(amount) AS total_amount, SUM(SUM(amount)) OVER (ORDER BY method) AS cumulative_amount
FROM payments
WHERE paid_at >= '2023-06-29'
GROUP BY method;
