SELECT
    t.is_flagged,
    t.merchant,
    a.balance,
    a.account_type,
    c.city
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.account_type = 'CREDIT' AND TO_DATE(t.txn_time) < '2025-07-01'
