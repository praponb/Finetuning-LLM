SELECT id, account_id, txn_time, txn_type FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time, id) AS rn FROM transactions) AS ranked WHERE rn <= 2;
