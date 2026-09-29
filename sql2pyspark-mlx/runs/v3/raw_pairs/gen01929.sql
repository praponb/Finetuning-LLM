SELECT t.id, t.account_id, a.opened_date, a.customer_id, a.status FROM transactions t INNER JOIN accounts a ON t.account_id = a.id WHERE t.amount < 2900 ORDER BY id DESC LIMIT 50
