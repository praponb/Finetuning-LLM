SELECT t.txn_time, a.opened_date, a.balance FROM transactions t INNER JOIN accounts a ON t.account_id = a.id;
