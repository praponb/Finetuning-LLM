WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        firmware,
        TRIM(firmware) AS clean_firmware,
        CEIL(temp_c) AS temp_c_whole
    FROM device_readings
    WHERE pressure IS NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        status_bits,
        firmware,
        temp_c_whole,
        MIN(temp_c_whole) OVER w_running AS last3_min_temp_c_whole,
        MAX(temp_c_whole) OVER w_running AS last3_max_temp_c_whole,
        COUNT(temp_c_whole) OVER w_running AS last3_count_temp_c_whole,
        AVG(temp_c_whole) OVER w_running AS last3_avg_temp_c_whole,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM valid
    WINDOW w_running AS (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY device_code ORDER BY status_bits, id)
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM enriched
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        status_bits,
        last3_max_temp_c_whole,
        last3_count_temp_c_whole,
        seq_no,
        SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
valid_device_readings AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        status_bits,
        last3_max_temp_c_whole,
        last3_count_temp_c_whole,
        seq_no,
        session_no,
        INITCAP(TRIM(device_code)) AS device_code_title
    FROM sessionized
    WHERE reading_ts >= TIMESTAMP '2025-09-01 07:00:00'
      AND device_code IN ('DV-02', 'DV-04', 'DV-03')
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY session_no DESC, id) AS row_num
        FROM valid_device_readings
    ) ranked
    WHERE row_num = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(last3_max_temp_c_whole * 0.9, 2) AS adjusted_last3_max_temp_c_whole
    FROM ranked
    WHERE reading_ts > DATE '2025-09-01'
      AND status_bits = 4
)
-- Aggregate with conditional metrics
SELECT
    session_no,
    COUNT(*) AS num_rows,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    COUNT_IF(device_code = 'DV-03') AS dv_03_events,
    COUNT(*) FILTER (WHERE device_code = 'DV-03') AS dv_03_rows,
    MAX(adjusted_last3_max_temp_c_whole) FILTER (WHERE device_code = 'DV-02') AS dv_02_max_adjusted_last3_max_temp_c_whole,
    SUM(CASE WHEN device_code = 'DV-02' THEN session_no ELSE 0 END) AS dv_02_session_no
FROM cleaned
GROUP BY session_no
