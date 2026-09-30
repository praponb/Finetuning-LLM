SELECT a.balance, a.opened_date, t.txn_type, t.amount FROM accounts a INNER JOIN transactions t ON t.account_id = a.id ORDER BY a.balance DESC, t.txn_type;
