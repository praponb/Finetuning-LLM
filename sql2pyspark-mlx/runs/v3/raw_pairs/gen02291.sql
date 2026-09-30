SELECT t.id, t.merchant, t.txn_type, a.opened_date FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id WHERE a.opened_date <= '2024-06-01';
