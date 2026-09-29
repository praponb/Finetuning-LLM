SELECT
    t.id,
    t.is_flagged,
    a.opened_date,
    a.status,
    a.customer_id,
    UPPER(a.account_type) AS account_type_upper
FROM transactions t
LEFT OUTER JOIN accounts a ON t.account_id = a.id
