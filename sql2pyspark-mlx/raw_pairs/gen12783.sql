SELECT id, store_id, amount, ROUND(amount / SUM(amount) OVER (PARTITION BY store_id), 3) AS amount_share FROM orders;
