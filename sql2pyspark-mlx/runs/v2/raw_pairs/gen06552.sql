SELECT
    t.id,
    t.account_id,
    t.txn_type,
    t.is_flagged,
    a.account_type,
    a.customer_id,
    a.status
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE t.txn_type <> 'REFUND' OR a.opened_date <= '2025-10-13'
