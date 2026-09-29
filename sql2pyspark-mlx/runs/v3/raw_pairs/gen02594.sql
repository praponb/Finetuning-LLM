SELECT
    t.txn_time,
    t.is_flagged,
    a.opened_date,
    c.email,
    c.signup_date
FROM transactions t
FULL OUTER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.opened_date > '2025-01-01'
