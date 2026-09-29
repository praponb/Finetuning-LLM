WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, v
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, k) > INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM flattened
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY service ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        log_ts,
        k,
        v,
        new_group_flag,
        MAX(new_group_flag) OVER w_recent AS last4_max_new_group_flag,
        MIN(new_group_flag) OVER w_recent AS last4_min_new_group_flag,
        AVG(new_group_flag) OVER w_recent AS last4_avg_new_group_flag,
        SUM(new_group_flag) OVER w_recent AS last4_sum_new_group_flag,
        AVG(island_id) OVER (PARTITION BY YEAR(log_ts), k ORDER BY log_ts, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS island_id_avg_3
    FROM sessions_app_logs
    WHERE new_group_flag > 0
    WINDOW w_recent AS (PARTITION BY service ORDER BY log_ts, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        log_ts,
        k,
        v,
        new_group_flag,
        last4_max_new_group_flag,
        island_id_avg_3,
        FIRST_VALUE(new_group_flag) IGNORE NULLS OVER (PARTITION BY k ORDER BY new_group_flag, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_new_group_flag,
        ROW_NUMBER() OVER (PARTITION BY service ORDER BY last4_max_new_group_flag DESC, id, k) AS row_seq,
        NTILE(4) OVER (PARTITION BY service ORDER BY island_id_avg_3 DESC, id, k) AS island_id_avg_3_bucket,
        ROW_NUMBER() OVER (PARTITION BY k, service ORDER BY log_ts, id, k) AS seq_no
    FROM metrics
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(island_id_avg_3_bucket AS DOUBLE) AS island_id_avg_3_bucket_str,
        COALESCE(k, 'OTHER') AS resolved_k
    FROM scored
    WHERE log_ts > TO_DATE('2025-06-01')
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        service,
        k,
        first_known_new_group_flag
    FROM valid
) src
PIVOT (
    MAX(first_known_new_group_flag)
    FOR k IN ('trace' AS trace, 'version' AS version)
);
