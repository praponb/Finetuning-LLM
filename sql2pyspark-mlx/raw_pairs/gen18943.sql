SELECT
    id,
    merchant,
    txn_time,
    amount,
    LEAD(amount, 1) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS next_amount
FROM transactions
WHERE txn_type IN ('DEBIT', 'FEE')
ORDER BY 2;
