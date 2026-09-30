WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, attr_value
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(host, ' - ', k) AS host_k,
        COALESCE(k, 'N/A') AS k_clean
    FROM exploded
    WHERE log_ts > TIMESTAMP '2025-06-01 15:00:00'
      AND k IS DISTINCT FROM 'version'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        YEAR(log_ts) AS log_ts_year,
        COALESCE(k, 'Unknown') AS k_filled,
        COALESCE(k, 'N/A') AS k_clean_2
    FROM valid
    WHERE log_ts > TO_TIMESTAMP('2025-06-01 14:00:00')
      AND k != 'version'
),
base_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        id,
        log_ts,
        attr_value,
        host_k,
        k_clean,
        log_ts_year,
        CONCAT(host, ', ', k) AS host_k_2,
        INITCAP(TRIM(k)) AS k_title
    FROM base
    WHERE log_ts > DATE '2025-06-01'
)
SELECT
    id,
    attr_value,
    host_k,
    k_clean,
    log_ts_year,
    host_k_2,
    k_title
FROM base_app_logs
ORDER BY id NULLS FIRST
