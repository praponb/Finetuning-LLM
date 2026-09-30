SELECT
    t.id,
    t.txn_type,
    a.balance,
    a.opened_date,
    a.customer_id
FROM transactions t
FULL OUTER JOIN accounts a ON t.account_id = a.id
WHERE t.is_flagged AND a.customer_id IN (2, 8, 21, 22)
ORDER BY txn_type;
