SELECT t.txn_time, a.opened_date, c.signup_date
FROM transactions t
FULL OUTER JOIN accounts a ON t.account_id = a.id
LEFT JOIN customers c ON a.customer_id = c.id
WHERE t.txn_time < '2023-10-01'
