SELECT id, store_id, channel, amount, ROUND(amount / SUM(amount) OVER (PARTITION BY store_id, channel), 3) amount_share FROM orders;
