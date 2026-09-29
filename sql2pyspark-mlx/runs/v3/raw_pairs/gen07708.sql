SELECT
    id,
    amount,
    txn_type,
    LAST_VALUE(txn_type) OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_txn_type
FROM transactions
WHERE txn_type IN ('REFUND', 'FEE', 'CREDIT')
