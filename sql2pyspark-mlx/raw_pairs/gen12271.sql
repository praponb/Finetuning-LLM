WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, attr_value
),
parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        status_code,
        key,
        attr_value,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE key = 'trace'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN status_code >= 411 THEN 'High' WHEN status_code >= 285 THEN 'Medium' ELSE 'Low' END AS status_code_band,
        LENGTH(key) AS key_length
    FROM parsed
    WHERE key IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        key,
        status_code_high,
        AVG(flag_bit1) OVER (PARTITION BY service ORDER BY flag_bit1, id, key ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS flag_bit1_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY service ORDER BY status_code, id, key) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY status_code_high DESC, id, key) AS row_seq_2,
        MIN(status_code_high) OVER (PARTITION BY key, service) AS key_min_status_code_high,
        SUM(status_code_high) OVER (PARTITION BY service ORDER BY flag_bit1, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_code_high
    FROM cleaned
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        key,
        row_seq,
        key_min_status_code_high,
        running_status_code_high,
        NTILE(10) OVER (ORDER BY key_min_status_code_high DESC, id, key) AS key_min_status_code_high_bucket,
        FIRST_VALUE(flag_bit1_avg_3_prior, TRUE) OVER (PARTITION BY key ORDER BY status_code_high, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_flag_bit1_avg_3_prior
    FROM with_windows
    WHERE status_code_high >= 2
)
SELECT
    id,
    key,
    row_seq,
    key_min_status_code_high,
    key_min_status_code_high_bucket,
    first_known_flag_bit1_avg_3_prior,
    CASE WHEN key RLIKE '^(version|region)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS key_zone
FROM enriched
ORDER BY key_min_status_code_high_bucket DESC NULLS FIRST, key
