WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, value
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        level,
        status_code,
        key,
        value,
        FIRST_VALUE(key) IGNORE NULLS OVER (PARTITION BY level ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_key,
        AVG(status_code) OVER (PARTITION BY service, key) AS service_avg_status_code
    FROM lines
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        level,
        status_code,
        key,
        service_avg_status_code,
        COUNT(service_avg_status_code) OVER (PARTITION BY service ORDER BY service_avg_status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_service_avg_status_code,
        SUM(service_avg_status_code) OVER (PARTITION BY service ORDER BY service_avg_status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_service_avg_status_code,
        MAX(service_avg_status_code) OVER (PARTITION BY service ORDER BY service_avg_status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_service_avg_status_code,
        AVG(service_avg_status_code) OVER (PARTITION BY service ORDER BY service_avg_status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_service_avg_status_code,
        FIRST_VALUE(service_avg_status_code, TRUE) OVER (PARTITION BY service ORDER BY service_avg_status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_service_avg_status_code
    FROM scored
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, level
    FROM metrics
    INTERSECT
    SELECT id, level
    FROM app_logs
    WHERE latency_ms != 2281
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN remaining_keys k ON s.id = k.id AND s.level = k.level
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        level,
        status_code,
        service_avg_status_code,
        running_count_service_avg_status_code,
        running_max_service_avg_status_code,
        first_known_service_avg_status_code,
        CUME_DIST() OVER (PARTITION BY key ORDER BY running_max_service_avg_status_code DESC NULLS FIRST) AS running_max_service_avg_status_code_cume_dist
    FROM kept_rows
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN service_avg_status_code >= 438 THEN 'HIGH' ELSE 'LOW' END AS service_avg_status_code_tier,
        level,
        SUM(CASE WHEN level = 'WARN' THEN status_code ELSE 0 END) AS warn_status_code,
        SUM(running_max_service_avg_status_code_cume_dist) AS total_running_max_service_avg_status_code_cume_dist,
        DENSE_RANK() OVER (PARTITION BY CASE WHEN service_avg_status_code >= 438 THEN 'HIGH' ELSE 'LOW' END ORDER BY SUM(CASE WHEN level = 'WARN' THEN status_code ELSE 0 END) DESC) AS warn_status_code_dense_rank
    FROM with_windows
    GROUP BY service_avg_status_code_tier, level
)
SELECT
    service_avg_status_code_tier,
    level,
    warn_status_code,
    total_running_max_service_avg_status_code_cume_dist
FROM totals
ORDER BY level DESC
