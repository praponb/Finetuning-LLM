SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) seq_no,
    COUNT(amount) OVER (PARTITION BY account_id, merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) amount_count_90d,
    ROUND(amount / SUM(amount) OVER (PARTITION BY merchant), 4) amount_share
FROM transactions
