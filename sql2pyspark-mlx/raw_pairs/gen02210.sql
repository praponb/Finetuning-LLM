SELECT t.id, t.account_id, a.customer_id FROM transactions t JOIN accounts a ON t.account_id = a.id WHERE a.balance <= 18000 ORDER BY t.id DESC;
