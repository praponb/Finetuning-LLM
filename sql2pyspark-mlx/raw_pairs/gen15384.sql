SELECT
    id,
    account_id,
    txn_type,
    txn_time,
    is_flagged,
    ROUND(amount / SUM(amount) OVER (PARTITION BY MONTH(txn_time), merchant), 4) amount_share,
    MAX(amount) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) amount_max_30d,
    AVG(amount) OVER (ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) amount_avg_3_prior,
    ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC, id) seq_no
FROM transactions;
