SELECT
    a.status,
    a.opened_date,
    t.amount,
    t.is_flagged
FROM accounts a
INNER JOIN transactions t ON a.id = t.account_id
