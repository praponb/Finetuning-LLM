WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'UNKNOWN') AS merchant_filled
    FROM transactions
    WHERE txn_time > DATE '2024-05-12'
      AND merchant IS NOT NULL
      AND TO_DATE(txn_time) >= '2024-06-01'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id, txn_type ORDER BY amount ASC NULLS LAST, id ASC) = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM latest
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        merchant,
        txn_type,
        session_id
    FROM sessions_transactions
) src
PIVOT (
    AVG(session_id) AS total,
    COUNT(1) AS item_count
    FOR txn_type IN ('REFUND' AS refund, 'FEE' AS fee)
);
