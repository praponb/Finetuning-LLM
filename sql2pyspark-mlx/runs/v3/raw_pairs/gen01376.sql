SELECT method, SUM(amount) total_amount, SUM(SUM(amount)) OVER (ORDER BY method) AS cumulative_amount
FROM payments
GROUP BY method
ORDER BY cumulative_amount DESC
