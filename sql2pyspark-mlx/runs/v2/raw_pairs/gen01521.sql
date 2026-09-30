SELECT t.id, t.amount, a.account_type, a.balance FROM transactions t JOIN accounts a ON t.account_id = a.id WHERE t.amount >= 4000 AND t.amount <= 2900 ORDER BY balance;
