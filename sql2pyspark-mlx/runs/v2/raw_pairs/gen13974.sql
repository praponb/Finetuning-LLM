SELECT a.account_type, MIN(t.amount) min_amount FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id WHERE a.account_type != 'LOAN' GROUP BY a.account_type
