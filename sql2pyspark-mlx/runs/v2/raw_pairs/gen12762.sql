SELECT
    id,
    account_id,
    txn_time,
    amount,
    ROUND(AVG(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_7,
    MIN(amount) OVER (PARTITION BY account_id) AS account_id_min_amount
FROM transactions;
