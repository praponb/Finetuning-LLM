SELECT
    id,
    merchant,
    txn_type,
    txn_time,
    CONCAT_WS('-', txn_type, merchant) AS txn_type_merchant_key
FROM transactions
WHERE amount <= 2800 AND merchant IN ('Lazada', 'Amazon') AND txn_type IN ('DEBIT', 'REFUND', 'FEE')
ORDER BY merchant DESC, txn_time ASC, id ASC
LIMIT 10;
