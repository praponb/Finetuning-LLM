WITH cleaned AS (
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        CONCAT_WS('-', merchant, txn_type) AS merchant_txn_type_key
    FROM transactions
    WHERE merchant = 'Starbucks'
      AND txn_time > TIMESTAMP '2023-05-29 10:00:00'
),
flagged AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
sessions_transactions AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
enriched AS (
    SELECT
        *,
        LAG(session_start_flag) OVER w_ordered AS prev_session_start_flag,
        LEAD(island_id) OVER w_ordered AS next_island_id,
        LAG(island_id) OVER w_ordered AS prev_island_id,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        LEAD(session_start_flag, 1, 0) OVER (PARTITION BY merchant ORDER BY session_start_flag, id) AS next_session_start_flag
    FROM sessions_transactions
    WINDOW w_ordered AS (PARTITION BY txn_type ORDER BY island_id, id)
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY prev_session_start_flag, id) AS seq_no,
        MIN(prev_session_start_flag) OVER (PARTITION BY merchant) AS merchant_min_prev_session_start_flag,
        RANK() OVER (PARTITION BY merchant ORDER BY next_session_start_flag NULLS FIRST) AS next_session_start_flag_rank,
        NTILE(10) OVER (ORDER BY prev_session_start_flag, id) AS prev_session_start_flag_bucket,
        SUM(next_session_start_flag) OVER (ORDER BY next_session_start_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_session_start_flag
    FROM enriched
)
SELECT
    id,
    account_id,
    txn_time,
    is_flagged,
    merchant_txn_type_key,
    session_start_flag,
    island_id,
    next_island_id,
    prev_island_id
FROM scored
