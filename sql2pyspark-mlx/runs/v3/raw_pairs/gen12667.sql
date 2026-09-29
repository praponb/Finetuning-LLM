SELECT
    t.txn_time,
    t.txn_type,
    t.is_flagged,
    a.customer_id,
    a.status,
    a.balance
FROM transactions t
LEFT OUTER JOIN accounts a ON t.account_id = a.id
WHERE t.txn_type NOT IN ('DEBIT', 'CREDIT') AND a.status = 'FROZEN';
