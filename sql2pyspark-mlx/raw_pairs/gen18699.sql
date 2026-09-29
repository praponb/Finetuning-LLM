WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('/', firmware, device_code) AS firmware_device_code_key
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 02:00:00'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CORR(pressure, status_bits) OVER (PARTITION BY device_code) AS corr_pressure_status_bits,
        SUM(status_bits) OVER w_grp AS running_status_bits
    FROM base
    WINDOW w_grp AS (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_burst
    FROM with_windows
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_windows_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        temp_c,
        status_bits,
        firmware,
        firmware_device_code_key,
        running_status_bits,
        session_no,
        AVG(humidity) OVER (PARTITION BY YEAR(reading_ts) ORDER BY pressure, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS humidity_avg_2_prior,
        RANK() OVER (PARTITION BY device_code ORDER BY is_new_burst DESC NULLS LAST) AS is_new_burst_rank,
        LAG(pressure, 1, 0) OVER (PARTITION BY device_code ORDER BY temp_c, id) AS prev_pressure,
        ROUND(pressure / SUM(pressure) OVER (PARTITION BY device_code), 4) AS pressure_share
    FROM islands
    WHERE device_code IN ('DV-01', 'DV-03', 'DV-02')
)
SELECT
    id,
    device_code,
    temp_c,
    firmware,
    firmware_device_code_key,
    running_status_bits,
    session_no,
    humidity_avg_2_prior,
    is_new_burst_rank,
    CASE WHEN device_code RLIKE '^(DV-02|DV-01)$' THEN 'GROUP_B' WHEN device_code RLIKE '^(DV-03|DV-04)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS device_code_zone
FROM with_windows_device_readings
ORDER BY humidity_avg_2_prior DESC NULLS LAST
