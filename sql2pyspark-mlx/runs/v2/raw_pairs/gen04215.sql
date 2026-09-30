SELECT status, 'account' AS source FROM accounts UNION SELECT status, 'payment' AS source FROM payments WHERE amount > 900 ORDER BY status DESC;
