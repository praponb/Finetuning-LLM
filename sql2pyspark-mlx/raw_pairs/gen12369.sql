WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        is_flagged,
        ROUND(amount * 1.07, 2) AS adjusted_amount,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'DIRECT') AS merchant_filled
    FROM transactions
    WHERE merchant IN ('Lazada', 'Shell', '7-Eleven')
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM cleaned
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Multi-level aggregation
SELECT
    CASE WHEN adjusted_amount >= 85 THEN 'LARGE' ELSE 'SMALL' END AS adjusted_amount_tier,
    CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_session_tier,
    AVG(session_id) FILTER (WHERE txn_type = 'CREDIT') AS credit_avg_session_id,
    COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END) AS debit_count,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(adjusted_amount, ARRAY(0.25, 0.5, 0.75)) AS adjusted_amount_quartiles
FROM sessions_transactions
GROUP BY CUBE (CASE WHEN adjusted_amount >= 85 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY credit_avg_session_id DESC NULLS FIRST
