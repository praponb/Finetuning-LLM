WITH lines AS (
    -- Unnest the array into one row per element
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
    LATERAL VIEW OUTER EXPLODE(context) x AS k, value
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(k, ' - ', host) AS k_host
    FROM lines
    WHERE host = 'h4'
      AND log_ts >= DATE '2025-06-01'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        value,
        k_host,
        ROUND(latency_ms / status_code, 2) AS latency_ms_per_status_code
    FROM filtered
    WHERE log_ts >= DATE '2025-06-01'
      AND k IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, host
    FROM cleaned
    EXCEPT DISTINCT
    SELECT id, host
    FROM app_logs
    WHERE latency_ms >= 2244
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN matched_keys k ON s.id = k.id AND s.host = k.host
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        k,
        value,
        k_host,
        CONCAT(k, '/', host) AS k_host_2,
        COALESCE(k, 'DIRECT') AS k_clean
    FROM verified
    WHERE log_ts > TIMESTAMP '2025-06-01 10:00:00'
)
SELECT
    log_ts,
    latency_ms,
    status_code,
    k,
    k_host,
    k_host_2,
    k_clean
FROM base
ORDER BY k
