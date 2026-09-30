SELECT order_date, 'order' AS source FROM orders UNION SELECT opened_date, 'account' AS source FROM accounts
