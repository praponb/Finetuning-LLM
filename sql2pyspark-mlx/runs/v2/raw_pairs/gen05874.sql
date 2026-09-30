SELECT a.status, COUNT(DISTINCT t.amount) unique_accounts, AVG(t.amount) avg_amount FROM transactions t INNER JOIN accounts a ON t.account_id = a.id GROUP BY a.status;
