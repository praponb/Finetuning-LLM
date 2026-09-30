WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, attr_value
),
unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        status_code,
        key,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
    WHERE status_code <= 352
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(flag_bit1) OVER (PARTITION BY key ORDER BY status_code_high, id, key) AS next_flag_bit1,
        LAG(flag_bit1) OVER (PARTITION BY key ORDER BY status_code_high, id, key) AS prev_flag_bit1,
        LEAD(status_code_high) OVER (PARTITION BY key ORDER BY status_code_high, id, key) AS next_status_code_high,
        LAG(status_code_high) OVER (PARTITION BY key ORDER BY status_code_high, id, key) AS prev_status_code_high,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY status_code_high, id, key) AS row_seq
    FROM unpacked
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        status_code,
        key,
        flag_bit1,
        status_code_high,
        next_flag_bit1,
        prev_flag_bit1,
        next_status_code_high,
        prev_status_code_high,
        row_seq,
        COALESCE(status_code_high, 0) AS status_code_high_filled,
        COALESCE(key, 'OTHER') AS resolved_key
    FROM enriched
    WHERE key IS NOT NULL
      AND key NOT IN ('trace', 'version')
),
enriched_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER w_recent AS status_code_cume_dist,
        AVG(status_code_high) OVER w_running AS status_code_high_avg_5_prior,
        AVG(next_flag_bit1) OVER w_recent_2 AS next_flag_bit1_avg_6
    FROM cleaned
    WINDOW w_recent AS (ORDER BY status_code DESC), w_running AS (PARTITION BY key ORDER BY prev_status_code_high, id, key ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_recent_2 AS (ORDER BY status_code, id, key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    key,
    kpi,
    value
FROM (
    SELECT
        id,
        key,
        status_code_high_filled,
        status_code_cume_dist
    FROM enriched_app_logs
) src
UNPIVOT (
    value FOR kpi IN (status_code_high_filled AS STATUS_CODE_HIGH_FILLED_VAL, status_code_cume_dist AS STATUS_CODE_CUME_DIST_VAL)
)
