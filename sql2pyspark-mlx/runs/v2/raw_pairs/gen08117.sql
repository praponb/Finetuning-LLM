SELECT
    id,
    txn_type,
    txn_time,
    account_id
FROM transactions
WHERE (account_id IN (6, 26, 39) AND account_id IN (6, 13, 19, 40)) OR HOUR(txn_time) BETWEEN 10 AND 16
ORDER BY txn_type DESC;
