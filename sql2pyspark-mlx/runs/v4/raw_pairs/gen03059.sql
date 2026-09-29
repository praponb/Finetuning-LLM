WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, value
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        key,
        value,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE status_code = 330
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        status_code,
        key,
        value,
        flag_bit8,
        status_code_high,
        CASE WHEN flag_bit8 >= 1 THEN 'High' WHEN flag_bit8 >= 0 THEN 'Medium' ELSE 'Low' END AS flag_bit8_band
    FROM flattened_attrs
    WHERE key IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        host,
        status_code,
        key,
        value,
        flag_bit8,
        status_code_high,
        flag_bit8_band,
        LEAD(flag_bit8, 1) OVER (ORDER BY status_code_high, id, key) AS next_flag_bit8,
        RANK() OVER (ORDER BY flag_bit8 DESC NULLS LAST) AS flag_bit8_rank,
        AVG(status_code_high) OVER (PARTITION BY host ORDER BY flag_bit8, id, key ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_code_high_avg_6_prior
    FROM base
)
SELECT
    id,
    host,
    status_code,
    key,
    flag_bit8,
    flag_bit8_band,
    next_flag_bit8,
    flag_bit8_rank,
    status_code_high_avg_6_prior,
    CASE WHEN key RLIKE '^(trace|version)$' THEN 'EXTENDED' WHEN key RLIKE '^(region)$' THEN 'CORE' ELSE 'GROUP_B' END AS key_zone
FROM with_windows
ORDER BY flag_bit8_rank;
