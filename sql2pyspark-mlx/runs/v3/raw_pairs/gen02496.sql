SELECT id, account_id, txn_time, merchant FROM (SELECT *, RANK() OVER (PARTITION BY account_id ORDER BY txn_time DESC) AS pos FROM transactions) AS ranked WHERE pos <= 2;
