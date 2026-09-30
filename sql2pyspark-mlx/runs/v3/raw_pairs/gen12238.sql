SELECT t.merchant, t.amount, t.txn_time, a.opened_date, a.balance, a.customer_id FROM transactions t INNER JOIN accounts a ON t.account_id = a.id;
