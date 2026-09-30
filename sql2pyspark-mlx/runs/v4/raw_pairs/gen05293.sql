SELECT id, txn_time, DENSE_RANK() OVER (ORDER BY txn_time) AS dense_rnk
FROM transactions
