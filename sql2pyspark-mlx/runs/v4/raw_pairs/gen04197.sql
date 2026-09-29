SELECT
    t.txn_type,
    a.balance,
    a.status,
    a.opened_date
FROM transactions t
JOIN accounts a ON t.account_id = a.id
WHERE NOT (t.txn_type = 'REFUND' OR t.amount < 1400);
