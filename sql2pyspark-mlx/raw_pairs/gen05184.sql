SELECT t.txn_time, a.status, c.is_active, c.city FROM transactions t LEFT OUTER JOIN accounts a ON t.account_id = a.id RIGHT OUTER JOIN customers c ON a.customer_id = c.id;
