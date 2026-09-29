SELECT * FROM (SELECT id, account_id, txn_time, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time, id) AS rnk FROM transactions WHERE amount < 1200) AS t WHERE rnk = 1 ORDER BY id
