SELECT status, SUM(amount) AS total_amount, SUM(SUM(amount)) OVER (ORDER BY status) AS cumulative_amount
FROM orders
GROUP BY status
ORDER BY total_amount DESC, status
LIMIT 5
