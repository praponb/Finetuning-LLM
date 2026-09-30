WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY reading_ts, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure, id) AS event_seq
    FROM device_readings
    WHERE status_bits > 10
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM metrics
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        seq_no,
        SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(seq_no) OVER (PARTITION BY device_code ORDER BY seq_no, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_seq_no,
        MIN(seq_no) OVER (PARTITION BY device_code ORDER BY seq_no, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_seq_no,
        AVG(seq_no) OVER (PARTITION BY device_code ORDER BY seq_no, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_seq_no,
        NTILE(10) OVER (PARTITION BY device_code ORDER BY seq_no DESC, id) AS seq_no_bucket
    FROM islands
),
with_windows_device_readings AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(last4_min_seq_no) OVER w_hist AS last4_count_last4_min_seq_no,
        MIN(last4_min_seq_no) OVER w_hist AS last4_min_last4_min_seq_no,
        MAX(last4_min_seq_no) OVER w_hist AS last4_max_last4_min_seq_no,
        SUM(status_bits) OVER w_ordered AS cumulative_status_bits
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY device_code ORDER BY last4_avg_seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_device_readings_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        humidity,
        pressure,
        status_bits,
        session_id,
        last4_min_seq_no,
        last4_avg_seq_no,
        last4_count_last4_min_seq_no,
        last4_max_last4_min_seq_no,
        cumulative_status_bits,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS pressure_sum_2h,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY last4_avg_seq_no, id) AS row_seq,
        SUM(pressure) OVER (PARTITION BY device_code) AS device_code_sum_pressure
    FROM with_windows_device_readings
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        device_code,
        pressure,
        last4_max_last4_min_seq_no
    FROM with_windows_device_readings_2
) src
UNPIVOT (
    value FOR measure IN (pressure, last4_max_last4_min_seq_no)
);
