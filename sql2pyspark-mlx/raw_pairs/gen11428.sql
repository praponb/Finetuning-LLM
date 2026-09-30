SELECT
    t.id,
    t.amount,
    t.is_flagged,
    a.opened_date,
    a.customer_id
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
