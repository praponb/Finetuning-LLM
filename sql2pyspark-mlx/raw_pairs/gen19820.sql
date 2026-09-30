WITH flattened AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, v
),
with_windows AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        key,
        AVG(status_code) OVER (PARTITION BY key ORDER BY status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_status_code,
        MIN(status_code) OVER (PARTITION BY key ORDER BY status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_status_code,
        SUM(status_code) OVER (PARTITION BY key ORDER BY status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_status_code,
        MAX(status_code) OVER (PARTITION BY key ORDER BY status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_status_code
    FROM flattened
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY status_code, id, key) AS latest_rank
        FROM with_windows
    ) x
    WHERE latest_rank = 1
),
matched_keys AS (
    SELECT id
    FROM latest
    INTERSECT
    SELECT id
    FROM app_logs
    WHERE level IN ('INFO', 'DEBUG', 'WARN')
),
kept_rows AS (
    SELECT s.*
    FROM latest s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    key,
    COUNT(*) FILTER (WHERE key = 'region') AS region_rows,
    APPROX_PERCENTILE(running_sum_status_code, 0.95) AS running_sum_status_code_p95,
    COUNT_IF(key = 'region') AS region_events,
    COUNT(CASE WHEN key = 'trace' THEN 1 END) AS trace_count,
    SUM(CASE WHEN key = 'version' THEN running_max_status_code ELSE 0 END) AS version_running_max_status_code
FROM kept_rows
GROUP BY key
HAVING region_rows >= 1;
