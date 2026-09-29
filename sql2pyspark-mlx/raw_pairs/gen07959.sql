SELECT t.id, t.txn_time, a.opened_date, a.account_type FROM transactions t JOIN accounts a ON t.account_id = a.id;
