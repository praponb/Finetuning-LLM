SELECT t.txn_time, a.opened_date, c.email
FROM transactions t
FULL OUTER JOIN accounts a ON a.id = t.account_id
LEFT JOIN customers c ON a.customer_id = c.id
