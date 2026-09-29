SELECT
    id,
    txn_type,
    account_id,
    amount,
    MAX(amount) OVER (PARTITION BY txn_type, account_id) AS txn_type_max_amount
FROM transactions
ORDER BY txn_type_max_amount DESC;
