SELECT
    id,
    txn_time,
    HOUR(txn_time) AS txn_time_hour,
    ROUND(amount, 0) AS amount_rounded
FROM transactions
WHERE HOUR(txn_time) BETWEEN 12 AND 18 AND account_id IN (19, 21) AND account_id IN (3, 19);
