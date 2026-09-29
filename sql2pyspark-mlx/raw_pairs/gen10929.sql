WITH exploded AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, value
),
valid AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        LENGTH(k) AS k_length
    FROM exploded
    WHERE log_ts >= TIMESTAMP '2025-06-01 13:00:00'
),
base AS (
    SELECT *, ROUND(latency_ms * 0.85, 2) AS latency_ms_adjusted
    FROM valid
    WHERE log_ts >= TIMESTAMP '2025-06-01 09:00:00'
),
cleaned AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        latency_ms_adjusted,
        DATEDIFF(CURRENT_DATE(), log_ts) / 365 AS years_since_log_ts
    FROM base
    WHERE log_ts > DATE '2025-06-01'
),
cleaned_app_logs AS (
    SELECT *, ROUND(status_code * 0.9, 2) AS status_code_adjusted
    FROM cleaned
    WHERE level IN ('WARN', 'ERROR')
)
SELECT level, status_code_adjusted
FROM cleaned_app_logs
ORDER BY level DESC;
