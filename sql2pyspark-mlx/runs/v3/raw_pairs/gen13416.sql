SELECT
    a.status,
    a.account_type,
    a.opened_date,
    t.merchant,
    t.account_id
FROM accounts a
LEFT OUTER JOIN transactions t ON a.id = t.account_id
WHERE t.account_id IN (10, 16, 24, 33)
ORDER BY a.opened_date DESC;
