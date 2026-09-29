SELECT status, 'order' AS source FROM orders UNION ALL SELECT status, 'account' AS source FROM accounts ORDER BY status DESC;
