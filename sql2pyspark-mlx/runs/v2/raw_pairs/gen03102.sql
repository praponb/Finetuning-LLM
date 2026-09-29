SELECT
    id,
    amount,
    merchant,
    TRIM(txn_type) AS clean_txn_type
FROM transactions
WHERE merchant IN ('Lazada', '7-Eleven', 'Amazon') AND txn_time >= '2023-08-31'
ORDER BY clean_txn_type DESC;
