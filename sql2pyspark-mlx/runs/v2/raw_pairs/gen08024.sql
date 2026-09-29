SELECT a.status, COUNT(DISTINCT t.amount) AS unique_accounts, SUM(t.amount) AS total_amount FROM transactions t INNER JOIN accounts a ON t.account_id = a.id WHERE a.balance >= 30000 GROUP BY a.status
