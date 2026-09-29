SELECT
    a.balance,
    t.merchant,
    t.txn_time,
    t.account_id,
    TRIM(t.txn_type) AS clean_txn_type
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
WHERE a.account_type NOT IN ('SAVINGS', 'CREDIT') OR t.amount >= 2700
