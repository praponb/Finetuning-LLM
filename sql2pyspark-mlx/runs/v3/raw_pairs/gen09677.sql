SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC, id) rnk FROM transactions) t WHERE rnk <= 5 ORDER BY txn_type
