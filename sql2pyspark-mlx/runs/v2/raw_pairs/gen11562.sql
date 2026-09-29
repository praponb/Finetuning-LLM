SELECT
    t.is_flagged,
    t.txn_type,
    t.merchant,
    a.customer_id,
    COALESCE(t.amount, 0) AS amount_filled
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
WHERE a.balance > 24000
