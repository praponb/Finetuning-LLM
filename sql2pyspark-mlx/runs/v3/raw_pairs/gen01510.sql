SELECT
    t.amount,
    t.is_flagged,
    t.txn_time,
    a.status
FROM transactions t
JOIN accounts a ON t.account_id = a.id
WHERE t.txn_type IN ('DEBIT', 'REFUND') AND YEAR(a.opened_date) = 2023
