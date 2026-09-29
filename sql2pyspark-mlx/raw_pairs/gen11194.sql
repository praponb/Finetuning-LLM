SELECT
    t.id,
    t.account_id,
    t.txn_time,
    a.balance,
    c.is_active,
    c.country
FROM transactions t
LEFT OUTER JOIN accounts a ON a.id = t.account_id
FULL OUTER JOIN customers c ON a.customer_id = c.id
WHERE c.is_active AND t.amount < 1800
ORDER BY txn_time
