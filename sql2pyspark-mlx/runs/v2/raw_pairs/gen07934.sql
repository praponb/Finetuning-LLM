SELECT id, account_id, amount, ROUND(amount / SUM(amount) OVER (PARTITION BY account_id), 3) AS amount_share FROM transactions;
