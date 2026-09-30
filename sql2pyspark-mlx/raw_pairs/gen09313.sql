SELECT
    id,
    account_id,
    txn_type,
    amount,
    merchant,
    RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC) amount_rank,
    FIRST_VALUE(amount, TRUE) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_amount,
    AVG(amount) OVER (PARTITION BY YEAR(txn_time) ORDER BY txn_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) amount_avg_3
FROM transactions
