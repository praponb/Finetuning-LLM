SELECT
    t.id,
    t.is_flagged,
    a.opened_date,
    a.balance
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE a.account_type = 'CHECKING'
ORDER BY a.opened_date, a.balance DESC
