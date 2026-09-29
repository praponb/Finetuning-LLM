SELECT
    t.is_flagged,
    a.status,
    c.country,
    c.age,
    DATE_TRUNC('month', a.opened_date) AS opened_date_period
FROM transactions t
INNER JOIN accounts a ON a.id = t.account_id
INNER JOIN customers c ON a.customer_id = c.id
WHERE t.txn_time < '2023-12-08';
