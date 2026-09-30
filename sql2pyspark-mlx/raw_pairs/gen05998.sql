SELECT channel, SUM(amount) AS total_amount, SUM(SUM(amount)) OVER (ORDER BY channel) AS cumulative_amount
FROM orders
GROUP BY channel
ORDER BY channel
LIMIT 10
