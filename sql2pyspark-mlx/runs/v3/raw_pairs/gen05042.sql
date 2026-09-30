SELECT
    t.id,
    t.is_flagged,
    a.opened_date,
    CONCAT_WS('-', t.merchant, t.txn_type) AS merchant_txn_type_key
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE t.is_flagged
ORDER BY merchant_txn_type_key
