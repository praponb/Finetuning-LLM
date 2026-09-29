SELECT
    t.id,
    t.is_flagged,
    t.amount,
    a.customer_id,
    a.status
FROM transactions t
JOIN accounts a ON t.account_id = a.id
