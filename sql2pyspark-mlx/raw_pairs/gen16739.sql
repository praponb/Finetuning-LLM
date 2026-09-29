SELECT
    a.status,
    t.account_id,
    t.is_flagged,
    t.merchant
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
