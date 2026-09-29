SELECT a.status, t.txn_time, t.is_flagged FROM accounts a JOIN transactions t ON a.id = t.account_id WHERE a.status IN ('CLOSED', 'OPEN') ORDER BY txn_time
