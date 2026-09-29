SELECT
    id,
    txn_time,
    merchant,
    amount,
    CONCAT(txn_type, ' - ', merchant) AS txn_type_merchant
FROM transactions
WHERE txn_type <> 'REFUND'
ORDER BY 1 ASC
LIMIT 10;
