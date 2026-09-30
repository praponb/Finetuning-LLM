WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM device_readings
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        temp_c,
        humidity,
        pressure,
        firmware,
        session_no,
        LEAD(status_bits) OVER w_recent next_status_bits,
        LEAD(pressure) OVER w_recent next_pressure,
        ROW_NUMBER() OVER w_recent seq_no
    FROM islands
    WHERE temp_c > 25
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY session_start_flag, id)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(next_pressure, 0) next_pressure_filled
    FROM scored
    WHERE seq_no BETWEEN 4 AND 6
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(temp_c) OVER (ORDER BY next_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_temp_c,
        NTILE(10) OVER (ORDER BY next_status_bits, id) next_status_bits_bucket,
        NTILE(5) OVER w_running pressure_bucket,
        AVG(temp_c) OVER w_hist temp_c_avg_2,
        SUM(seq_no) OVER (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_seq_no
    FROM base
    WINDOW w_running AS (ORDER BY pressure DESC, id), w_hist AS (ORDER BY seq_no, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    session_no,
    APPROX_PERCENTILE(next_status_bits_bucket, ARRAY(0.25, 0.5, 0.75)) AS next_status_bits_bucket_quartiles,
    ROUND(AVG(session_no), 2) AS avg_session_no,
    SUM(humidity) AS total_humidity,
    APPROX_PERCENTILE(pressure, ARRAY(0.25, 0.5, 0.75)) AS pressure_quartiles,
    APPROX_PERCENTILE(humidity, 0.5) AS humidity_p50
FROM enriched
GROUP BY session_no
