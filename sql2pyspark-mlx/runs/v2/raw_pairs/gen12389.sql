SELECT t.id, t.account_id, a.opened_date, a.balance, c.country FROM transactions t INNER JOIN accounts a ON a.id = t.account_id LEFT OUTER JOIN customers c ON c.id = a.customer_id ORDER BY id DESC
