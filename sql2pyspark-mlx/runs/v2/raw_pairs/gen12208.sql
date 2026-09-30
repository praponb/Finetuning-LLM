SELECT TO_DATE(txn_time) AS txn_day, SUM(amount) AS total_amount, DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS amount_rank
FROM transactions
WHERE account_id = 32
GROUP BY TO_DATE(txn_time);
