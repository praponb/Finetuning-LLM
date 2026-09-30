SELECT t.id, t.account_id, t.txn_type, a.account_type, c.age FROM transactions t JOIN accounts a ON t.account_id = a.id JOIN customers c ON a.customer_id = c.id WHERE NOT t.is_flagged;
