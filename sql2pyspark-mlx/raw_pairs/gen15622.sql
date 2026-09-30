WITH flattened AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
),
parsed AS (
    SELECT SHIFTRIGHT(status_code, 2) AS status_code_high, CAST((status_code & 1) != 0 AS INT) AS flag_bit1
    FROM flattened
),
cleaned AS (
    SELECT *, CEIL(status_code_high) AS status_code_high_whole
    FROM parsed
    WHERE status_code_high > 2
),
with_windows AS (
    SELECT *, RANK() OVER (ORDER BY flag_bit1 DESC NULLS FIRST) AS flag_bit1_rank
    FROM cleaned
),
valid AS (
    SELECT *, FLOOR(status_code_high_whole) AS status_code_high_whole_whole
    FROM with_windows
    WHERE status_code_high > 1
),
cleaned_app_logs AS (
    SELECT
        *,
        GREATEST(status_code_high_whole, status_code_high) AS greatest_status_code_high_whole_status_code_high,
        status_code_high + status_code_high_whole AS status_code_high_plus_status_code_high_whole
    FROM valid
    WHERE flag_bit1_rank <= 3
),
cleaned_app_logs_2 AS (
    SELECT
        status_code_high,
        flag_bit1_rank,
        status_code_high_whole_whole,
        greatest_status_code_high_whole_status_code_high,
        status_code_high_plus_status_code_high_whole,
        flag_bit1 / NULLIF(status_code_high, 0) AS flag_bit1_per_status_code_high,
        GREATEST(status_code_high_whole_whole, greatest_status_code_high_whole_status_code_high) AS greatest_status_code_high_whole_whole_greatest_status_code_high_whole_status_code_high
    FROM cleaned_app_logs
    WHERE status_code_high_whole_whole BETWEEN 67 AND 75
)
SELECT
    status_code_high,
    flag_bit1_rank,
    status_code_high_whole_whole,
    greatest_status_code_high_whole_status_code_high,
    flag_bit1_per_status_code_high,
    greatest_status_code_high_whole_whole_greatest_status_code_high_whole_status_code_high
FROM cleaned_app_logs_2;
