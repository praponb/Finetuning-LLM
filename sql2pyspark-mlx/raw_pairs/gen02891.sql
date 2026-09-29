WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
valid AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        session_start_flag,
        burst_id,
        LENGTH(merchant) AS merchant_length,
        session_start_flag + amount AS session_start_flag_plus_amount,
        COALESCE(merchant, 'UNKNOWN') AS merchant_clean
    FROM islands
    WHERE txn_time >= DATE '2023-10-09'
      AND merchant IS NOT NULL
),
gaps AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
sessions_transactions AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
with_gaps_2 AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag_2
    FROM sessions_transactions
),
sessionized AS (
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        session_start_flag,
        merchant_length,
        merchant_clean,
        is_new_session,
        session_no,
        SUM(session_start_flag_2) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM with_gaps_2
)
SELECT
    id,
    account_id,
    txn_type,
    txn_time,
    merchant_length,
    merchant_clean,
    is_new_session,
    session_no,
    burst_id_2
FROM sessionized
