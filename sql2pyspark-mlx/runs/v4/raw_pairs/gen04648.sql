SELECT a.status, COUNT(t.amount) AS amount_count FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id GROUP BY a.status ORDER BY status;
