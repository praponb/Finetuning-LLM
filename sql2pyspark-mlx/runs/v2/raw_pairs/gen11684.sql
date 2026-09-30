SELECT
    id,
    txn_time,
    account_id,
    FIRST_VALUE(account_id) OVER (ORDER BY txn_time DESC, id) AS first_account_id
FROM transactions;
