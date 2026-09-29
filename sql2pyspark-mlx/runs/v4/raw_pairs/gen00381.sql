WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        HOUR(txn_time) AS txn_time_hour
    FROM transactions
    WHERE account_id IN (15, 17, 25, 29)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM sessionized
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY amount DESC NULLS LAST, id) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY session_no DESC, id) AS event_seq,
        NTILE(10) OVER (PARTITION BY txn_type ORDER BY amount, id) AS amount_bucket,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time DESC, id) AS row_seq,
        COUNT(new_group_flag) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS new_group_flag_count_7d,
        LEAD(new_group_flag, 1) OVER (PARTITION BY merchant ORDER BY new_group_flag, id) AS next_new_group_flag
    FROM deduped
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        d.status,
        d.account_type
    FROM scored s
    LEFT JOIN accounts d
        ON s.account_id = d.id
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_type ORDER BY txn_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM enriched
),
sessionized_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY account_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, merchant
    FROM sessionized_transactions
    EXCEPT DISTINCT
    SELECT id, merchant
    FROM transactions
    WHERE is_flagged
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessionized_transactions s
    INNER JOIN matched_keys k ON s.id = k.id AND s.merchant = k.merchant
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    CASE WHEN new_group_flag >= 1 THEN 'LARGE' ELSE 'SMALL' END AS new_group_flag_tier,
    CASE WHEN amount >= 3200 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    STDDEV_SAMP(amount) AS amount_stddev,
    SUM(CASE WHEN txn_type = 'FEE' THEN new_group_flag_count_7d ELSE 0 END) AS fee_new_group_flag_count_7d,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN status = 'OPEN' THEN 1 END), 0), 2) AS amount_per_open,
    APPROX_PERCENTILE(next_new_group_flag, ARRAY(0.25, 0.5, 0.75)) AS next_new_group_flag_quartiles,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', txn_time) ORDER BY SUM(CASE WHEN txn_type = 'FEE' THEN new_group_flag_count_7d ELSE 0 END) DESC) AS fee_new_group_flag_count_7d_dense_rank
FROM verified
GROUP BY DATE_TRUNC('YEAR', txn_time), CASE WHEN new_group_flag >= 1 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN amount >= 3200 THEN 'HIGH' ELSE 'LOW' END
ORDER BY duration_min;
