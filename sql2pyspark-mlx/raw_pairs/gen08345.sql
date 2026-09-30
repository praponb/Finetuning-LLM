SELECT
    id,
    account_id,
    txn_time,
    merchant,
    FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
    MIN(amount) OVER (PARTITION BY account_id) AS account_id_min_amount,
    MIN(amount) OVER (PARTITION BY account_id) AS account_id_min_amount_2,
    ROW_NUMBER() OVER (PARTITION BY merchant, txn_type ORDER BY txn_time DESC, id) AS seq_no
FROM transactions
WHERE account_id = 28;
