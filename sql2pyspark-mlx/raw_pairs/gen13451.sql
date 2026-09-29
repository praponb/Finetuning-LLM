SELECT
    account_id,
    is_flagged,
    amount_range,
    min_txn_time
FROM (
    SELECT
        account_id,
        is_flagged,
        MAX(amount) - MIN(amount) AS amount_range,
        MIN(txn_time) AS min_txn_time
    FROM transactions
    GROUP BY account_id, is_flagged
) AS x
WHERE amount_range >= 37
ORDER BY amount_range;
