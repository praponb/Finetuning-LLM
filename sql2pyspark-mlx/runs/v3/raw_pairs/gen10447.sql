SELECT t.txn_time, a.opened_date, a.status, c.is_active FROM transactions t FULL OUTER JOIN accounts a ON a.id = t.account_id INNER JOIN customers c ON c.id = a.customer_id;
