SELECT txn_type, SUM(amount) total_amount, RANK() OVER (ORDER BY SUM(amount) DESC) AS amount_rank FROM transactions WHERE amount <= 2300 GROUP BY txn_type;
