WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'DIRECT') AS merchant_filled
    FROM transactions
    WHERE txn_type = 'DEBIT'
      AND merchant IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(is_flagged_flag) OVER (PARTITION BY txn_type, account_id ORDER BY is_flagged_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_is_flagged_flag,
        MIN(is_flagged_flag) OVER (PARTITION BY txn_type, account_id ORDER BY is_flagged_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_is_flagged_flag,
        SUM(is_flagged_flag) OVER (PARTITION BY txn_type, account_id ORDER BY is_flagged_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_is_flagged_flag
    FROM valid
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        b.status,
        b.opened_date
    FROM enriched a
    LEFT JOIN accounts b
        ON a.account_id = b.id
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_dims
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, status ORDER BY running_count_is_flagged_flag DESC, id) AS rn
        FROM islands
    ) ranked
    WHERE rn = 1
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM deduped
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        running_count_is_flagged_flag,
        running_sum_is_flagged_flag,
        status,
        opened_date,
        burst_id,
        rn,
        SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM with_gaps_2
),
with_gaps_3 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM sessionized
),
islands_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps_3
)
-- Multi-level aggregation
SELECT
    CASE WHEN burst_id >= 8 THEN 'LARGE' ELSE 'SMALL' END AS burst_id_tier,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    merchant,
    SUM(burst_id) FILTER (WHERE txn_type = 'CREDIT') AS credit_total_burst_id,
    AVG(burst_id) FILTER (WHERE status = 'CLOSED') AS closed_avg_burst_id,
    COUNT_IF(txn_type = 'REFUND') AS refund_events
FROM islands_transactions
GROUP BY ROLLUP (CASE WHEN burst_id >= 8 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', opened_date), merchant)
HAVING refund_events > 1
