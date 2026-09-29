SELECT channel, SUM(amount) AS total_amount, RANK() OVER (ORDER BY SUM(amount) DESC) AS amount_rank
FROM orders
GROUP BY channel;
