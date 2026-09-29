WITH lines AS (
    SELECT
        id,
        service,
        host,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, value
),
unpacked AS (
    SELECT
        id,
        service,
        key,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
    WHERE key = 'version'
),
scored AS (
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY service ORDER BY flag_bit8 DESC, id, key) AS flag_bit8_bucket,
        FIRST_VALUE(status_code_high) IGNORE NULLS OVER (PARTITION BY key, service ORDER BY flag_bit8, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status_code_high
    FROM unpacked
    WHERE flag_bit8 = 0
),
metrics AS (
    SELECT
        *,
        COUNT(flag_bit8) OVER (PARTITION BY service, key ORDER BY status_code_high, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_flag_bit8,
        MAX(flag_bit8) OVER (PARTITION BY service, key ORDER BY status_code_high, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_flag_bit8,
        MIN(flag_bit8) OVER (PARTITION BY service, key ORDER BY status_code_high, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_flag_bit8
    FROM scored
),
matched_keys AS (
    SELECT service, id
    FROM metrics
    INTERSECT
    SELECT service, id
    FROM app_logs
    WHERE TO_DATE(log_ts) >= '2025-05-22'
),
reconciled AS (
    SELECT s.*
    FROM metrics s
    JOIN matched_keys k ON s.service = k.service AND s.id = k.id
),
metrics_app_logs AS (
    SELECT *, STDDEV_SAMP(first_known_status_code_high) OVER (PARTITION BY key, service) AS key_stddev_samp_first_known_status_code_high
    FROM reconciled
)
SELECT
    service,
    SUM(CASE WHEN service = 'search' THEN key_stddev_samp_first_known_status_code_high ELSE 0 END) AS search_key_stddev_samp_first_known_status_code_high,
    AVG(key_stddev_samp_first_known_status_code_high) AS avg_key_stddev_samp_first_known_status_code_high,
    COUNT(*) FILTER (WHERE service = 'search') AS search_rows,
    COUNT(CASE WHEN service = 'billing' THEN 1 END) AS billing_count,
    COUNT(DISTINCT service) AS distinct_services,
    AVG(first_known_status_code_high) AS avg_first_known_status_code_high
FROM metrics_app_logs
GROUP BY service
ORDER BY search_rows
