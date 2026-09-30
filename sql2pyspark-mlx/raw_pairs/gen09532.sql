WITH flattened AS (
    SELECT
        id,
        log_ts,
        level,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, value
),
parsed AS (
    SELECT
        id,
        log_ts,
        level,
        attr_key,
        CAST((status_code & 1) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
filtered AS (
    SELECT
        id,
        log_ts,
        level,
        attr_key,
        has_fault,
        status_code_high,
        CEIL(status_code_high) AS status_code_high_whole
    FROM parsed
    WHERE log_ts > TO_TIMESTAMP('2025-06-01 07:00:00')
      AND attr_key IS NOT NULL
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY status_code_high_whole DESC NULLS FIRST, id, attr_key) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    SELECT
        id,
        attr_key,
        NTILE(3) OVER w_running AS status_code_high_whole_bucket,
        DENSE_RANK() OVER w_running_2 AS status_code_high_dense_rank
    FROM ranked
    WINDOW w_running AS (PARTITION BY level ORDER BY status_code_high_whole DESC, id, attr_key), w_running_2 AS (PARTITION BY DATE_TRUNC('week', log_ts), level ORDER BY status_code_high NULLS FIRST)
),
metrics AS (
    SELECT
        *,
        FIRST_VALUE(status_code_high_dense_rank, TRUE) OVER (ORDER BY status_code_high_dense_rank, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status_code_high_dense_rank,
        MIN(status_code_high_dense_rank) OVER (PARTITION BY attr_key) AS attr_key_min_status_code_high_dense_rank,
        LAST_VALUE(status_code_high_dense_rank, TRUE) OVER (ORDER BY status_code_high_dense_rank, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status_code_high_dense_rank
    FROM with_windows
    WHERE status_code_high_dense_rank > 7
)
SELECT
    id,
    attr_key,
    status_code_high_whole_bucket,
    status_code_high_dense_rank,
    first_known_status_code_high_dense_rank,
    attr_key_min_status_code_high_dense_rank,
    last_known_status_code_high_dense_rank,
    CASE WHEN attr_key RLIKE '^(version|region)$' THEN 'CORE' WHEN attr_key RLIKE '^(trace)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS attr_key_zone
FROM metrics
ORDER BY attr_key_min_status_code_high_dense_rank DESC
