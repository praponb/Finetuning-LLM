WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        txn_time,
        merchant,
        is_flagged,
        SUM(amount) OVER (ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        FIRST_VALUE(amount) IGNORE NULLS OVER w_hist AS first_known_amount,
        LAST_VALUE(amount, TRUE) OVER w_running AS last_known_amount
    FROM transactions
    WHERE merchant = 'Lazada'
    WINDOW w_hist AS (PARTITION BY account_id, merchant ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(last_known_amount / cumulative_amount, 2) AS last_known_amount_per_cumulative_amount,
        COALESCE(merchant, 'UNKNOWN') AS merchant_filled
    FROM scored
    WHERE txn_time > TIMESTAMP '2023-10-07 07:00:00'
      AND merchant IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(cumulative_amount, 2, 0) OVER (PARTITION BY merchant ORDER BY last_known_amount_per_cumulative_amount ASC, id ASC) AS next_cumulative_amount,
        LAST_VALUE(last_known_amount, TRUE) OVER (PARTITION BY merchant ORDER BY cumulative_amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_known_amount,
        FIRST_VALUE(first_known_amount, TRUE) OVER (ORDER BY first_known_amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_known_amount,
        ROW_NUMBER() OVER (ORDER BY last_known_amount ASC, id ASC) AS event_seq
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM metrics
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_time,
        merchant,
        is_flagged,
        cumulative_amount,
        first_known_amount,
        last_known_amount,
        merchant_filled,
        last_known_last_known_amount,
        first_known_first_known_amount,
        event_seq,
        SUM(is_new_burst) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM sessionized
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_time,
        merchant,
        is_flagged,
        cumulative_amount,
        first_known_amount,
        last_known_amount,
        merchant_filled,
        first_known_first_known_amount,
        session_no,
        SUM(is_new_burst) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no_2
    FROM with_gaps
)
SELECT
    id,
    merchant,
    cumulative_amount,
    first_known_amount,
    last_known_amount,
    merchant_filled,
    first_known_first_known_amount,
    session_no,
    session_no_2
FROM islands
ORDER BY last_known_amount DESC NULLS LAST;
