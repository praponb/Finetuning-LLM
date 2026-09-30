SELECT
    id,
    txn_type,
    is_flagged,
    merchant,
    FLOOR(amount) AS amount_whole
FROM transactions
WHERE TO_DATE(txn_time) < '2023-09-01' AND txn_time < '2023-07-31';
