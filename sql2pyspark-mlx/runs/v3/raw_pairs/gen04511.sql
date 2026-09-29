SELECT
    a.status,
    a.opened_date,
    t.merchant,
    t.is_flagged,
    t.account_id
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
ORDER BY a.opened_date, t.merchant;
