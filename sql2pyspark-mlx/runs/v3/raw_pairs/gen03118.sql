SELECT
    id,
    txn_type,
    amount,
    merchant,
    FIRST_VALUE(merchant) OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS first_merchant
FROM transactions
ORDER BY txn_type
