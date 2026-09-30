SELECT id, channel, amount, ROUND(amount / SUM(amount) OVER (PARTITION BY channel), 3) AS amount_share FROM orders;
