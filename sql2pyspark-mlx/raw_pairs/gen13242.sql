SELECT
    id,
    txn_type,
    merchant,
    is_flagged
FROM transactions
WHERE txn_time < '2023-05-01'
ORDER BY merchant DESC, id DESC;
