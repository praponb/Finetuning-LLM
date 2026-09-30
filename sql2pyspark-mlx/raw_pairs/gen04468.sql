SELECT t.id, t.txn_time, a.balance, c.country FROM transactions t INNER JOIN accounts a ON a.id = t.account_id LEFT JOIN customers c ON a.customer_id = c.id ORDER BY t.id DESC
