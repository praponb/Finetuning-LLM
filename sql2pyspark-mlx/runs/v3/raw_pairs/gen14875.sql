SELECT
    id,
    account_id,
    txn_type,
    CAST(txn_time AS DATE) AS txn_time_day
FROM transactions
WHERE merchant != '7-Eleven';
