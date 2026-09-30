SELECT t.id, t.txn_time, t.amount, t.txn_type, a.opened_date FROM transactions t FULL OUTER JOIN accounts a ON t.account_id = a.id ORDER BY amount;
