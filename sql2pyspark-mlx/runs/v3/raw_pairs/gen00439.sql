SELECT channel, SUM(amount) AS total_amount, SUM(SUM(amount)) OVER (ORDER BY channel) AS cumulative_amount FROM orders WHERE amount <= 1040 GROUP BY channel;
