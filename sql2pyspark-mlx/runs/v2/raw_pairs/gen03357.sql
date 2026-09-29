SELECT a.customer_id, a.opened_date, t.amount, t.txn_type FROM accounts a JOIN transactions t ON t.account_id = a.id WHERE t.txn_type <> 'DEBIT' ORDER BY a.opened_date DESC
