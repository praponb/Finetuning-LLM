WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(temp_c AS INT) AS temp_c_int,
        CASE WHEN device_code IN ('DV-02', 'DV-03') THEN 'Core' ELSE 'Other' END AS device_code_group
    FROM sessions_device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 02:00:00'
      AND session_no <= 8
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        firmware,
        is_new_burst,
        session_no,
        temp_c_int,
        device_code_group,
        AVG(session_no) OVER (PARTITION BY MONTH(reading_ts), device_code ORDER BY reading_ts, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS session_no_avg_3,
        SUM(status_bits) OVER (PARTITION BY device_code ORDER BY session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_bits
    FROM filtered
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(is_new_burst) OVER (PARTITION BY device_code ORDER BY temp_c, id) AS next_is_new_burst,
        LAG(is_new_burst) OVER (PARTITION BY device_code ORDER BY temp_c, id) AS prev_is_new_burst,
        LAG(session_no) OVER (PARTITION BY device_code ORDER BY temp_c, id) AS prev_session_no,
        LEAD(session_no) OVER (PARTITION BY device_code ORDER BY temp_c, id) AS next_session_no,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c, id) AS row_seq
    FROM enriched
    WHERE reading_ts >= '2023-06-01'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY prev_session_no NULLS LAST, id) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
)
-- Ranking and gap features
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    temp_c_int,
    session_no_avg_3,
    running_status_bits,
    row_num,
    LAG(session_no_avg_3, 1) OVER (PARTITION BY device_code ORDER BY next_is_new_burst, id) AS prev_session_no_avg_3,
    LAST_VALUE(temp_c_int) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY prev_session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_temp_c_int,
    ROW_NUMBER() OVER (PARTITION BY TO_DATE(reading_ts), device_code ORDER BY temp_c_int DESC, id) AS row_seq_2,
    AVG(temp_c_int) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS temp_c_int_avg_30m,
    LAG(temp_c_int, 2) OVER (ORDER BY session_no_avg_3, id) AS prev_temp_c_int
FROM latest
