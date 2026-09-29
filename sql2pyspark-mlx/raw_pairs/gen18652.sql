WITH with_gaps AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
islands AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    COUNT(*) FILTER (WHERE txn_type = 'REFUND') AS refund_rows,
    COUNT_IF(txn_type = 'DEBIT') AS debit_events,
    SUM(CASE WHEN txn_type = 'FEE' THEN session_no ELSE 0 END) AS fee_session_no,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types,
    GROUPING_ID() AS gid
FROM islands
GROUP BY ROLLUP (DATE_TRUNC('WEEK', txn_time))
HAVING refund_rows > 1
ORDER BY refund_rows DESC;
