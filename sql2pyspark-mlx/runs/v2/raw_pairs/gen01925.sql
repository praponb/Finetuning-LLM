SELECT
    id,
    amount,
    merchant,
    txn_time,
    ROUND(amount * 100, 2) AS adjusted_amount
FROM transactions
WHERE txn_type IN ('CREDIT', 'FEE')
