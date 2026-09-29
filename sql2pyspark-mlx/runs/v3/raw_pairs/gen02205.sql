SELECT
    t.id,
    t.is_flagged,
    t.amount,
    t.txn_type,
    a.balance,
    a.opened_date,
    UPPER(a.account_type) AS account_type_upper
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
ORDER BY amount DESC;
