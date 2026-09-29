WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, v
),
unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        status_code,
        key,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        key,
        status_code_high,
        LEAD(status_code) OVER w_hist next_status_code,
        LAG(status_code) OVER w_hist prev_status_code,
        LAG(status_code_high) OVER w_hist prev_status_code_high,
        LEAD(status_code_high) OVER w_hist next_status_code_high,
        ROW_NUMBER() OVER w_hist seq_no,
        LAG(status_code, 2) OVER (PARTITION BY key ORDER BY flag_bit8, id, key) prev_status_code_2
    FROM unpacked
    WINDOW w_hist AS (PARTITION BY key ORDER BY status_code, id, key)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(next_status_code AS DOUBLE) next_status_code_num,
        COALESCE(key, 'NONE') resolved_key
    FROM enriched
    WHERE key IS NOT NULL
),
enriched_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(key, TRUE) OVER (PARTITION BY key ORDER BY next_status_code_num, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_key,
        MIN(status_code_high) OVER (PARTITION BY key) key_min_status_code_high,
        PERCENT_RANK() OVER w_ordered prev_status_code_percent_rank
    FROM valid
    WINDOW w_ordered AS (PARTITION BY key ORDER BY prev_status_code DESC NULLS LAST)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY seq_no DESC, id, key) AS row_num
    FROM enriched_app_logs
    QUALIFY row_num = 1
)
SELECT
    id,
    key,
    status_code_high,
    next_status_code,
    prev_status_code,
    prev_status_code_high,
    next_status_code_high,
    seq_no,
    next_status_code_num
FROM deduped
ORDER BY status_code_high DESC
