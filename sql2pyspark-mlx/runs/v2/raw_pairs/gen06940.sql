SELECT a.account_type, ROUND(STDDEV(t.amount), 2) AS stddev_amount FROM transactions t JOIN accounts a ON t.account_id = a.id GROUP BY a.account_type ORDER BY stddev_amount, account_type LIMIT 5
