SELECT
    id,
    is_flagged,
    amount,
    account_id,
    HOUR(txn_time) txn_time_hour
FROM transactions
