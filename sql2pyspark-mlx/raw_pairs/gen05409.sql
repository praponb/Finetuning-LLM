SELECT
    id,
    txn_time,
    merchant,
    is_flagged,
    MIN(amount) OVER (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_amount,
    AVG(amount) OVER (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_amount,
    MAX(amount) OVER (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_amount,
    LAST_VALUE(amount, TRUE) OVER (PARTITION BY account_id, merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount
FROM transactions;
