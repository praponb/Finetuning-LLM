SELECT t.txn_time, a.opened_date, a.balance, a.account_type FROM transactions t FULL OUTER JOIN accounts a ON t.account_id = a.id;
