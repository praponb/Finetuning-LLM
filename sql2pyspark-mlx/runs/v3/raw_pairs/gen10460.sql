SELECT status, 'order' AS source FROM orders UNION SELECT status, 'account' AS source FROM accounts ORDER BY status
