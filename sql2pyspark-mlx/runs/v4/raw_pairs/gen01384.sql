SELECT
    id,
    account_id,
    amount,
    txn_type,
    NTILE(10) OVER (PARTITION BY account_id ORDER BY amount, id) AS amount_ntile,
    ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS rn
FROM transactions
