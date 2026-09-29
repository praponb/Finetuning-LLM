SELECT id, account_id, txn_time, RANK() OVER (PARTITION BY account_id ORDER BY txn_time) AS rnk FROM transactions ORDER BY account_id DESC
