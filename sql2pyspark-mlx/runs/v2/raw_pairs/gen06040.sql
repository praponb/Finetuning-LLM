SELECT
    id,
    account_id,
    txn_type,
    amount,
    MAX(amount) OVER (PARTITION BY account_id, txn_type) account_id_max_amount
FROM transactions
