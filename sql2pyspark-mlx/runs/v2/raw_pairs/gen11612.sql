SELECT t.id, t.amount, t.txn_type, a.opened_date, c.country, c.city FROM transactions t JOIN accounts a ON a.id = t.account_id JOIN customers c ON c.id = a.customer_id
