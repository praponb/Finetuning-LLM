SELECT id, merchant, txn_time, DENSE_RANK() OVER (PARTITION BY merchant ORDER BY txn_time) AS dense_rnk FROM transactions ORDER BY id ASC;
