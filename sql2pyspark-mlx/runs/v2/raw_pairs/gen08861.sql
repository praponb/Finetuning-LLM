SELECT t.id, t.merchant, a.account_type FROM transactions t JOIN accounts a ON t.account_id = a.id WHERE t.is_flagged = TRUE AND a.opened_date < '2023-12-01' ORDER BY t.id DESC
