SELECT
    t.id,
    t.is_flagged,
    t.amount,
    t.merchant,
    a.customer_id,
    a.account_type
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
