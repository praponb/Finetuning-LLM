SELECT t.txn_time, t.txn_type, a.status, INITCAP(TRIM(t.txn_type)) AS txn_type_title FROM transactions t LEFT JOIN accounts a ON a.id = t.account_id WHERE t.account_id = 26;
