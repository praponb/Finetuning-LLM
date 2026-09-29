WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(status_bits, 1) AS status_bits_rounded,
        TO_DATE(reading_ts) AS reading_ts_date
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 07:00:00'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        reading_ts_date,
        CORR(pressure, humidity) OVER (PARTITION BY device_code) AS corr_pressure_humidity,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_bits_avg_30m,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits_rounded, id) AS event_seq,
        ROUND(temp_c / SUM(temp_c) OVER (PARTITION BY device_code), 4) AS temp_c_share
    FROM filtered
),
enriched_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        reading_ts_date,
        corr_pressure_humidity,
        status_bits_avg_30m,
        AVG(corr_pressure_humidity) OVER (PARTITION BY YEAR(reading_ts_date) ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS corr_pressure_humidity_avg_30m,
        LAG(humidity, 1) OVER w_running AS prev_humidity
    FROM enriched
    WHERE TO_DATE(reading_ts) >= '2023-10-01'
    WINDOW w_running AS (ORDER BY reading_ts, id)
),
base AS (
    -- Keep recent, valid rows
    SELECT *, temp_c * humidity AS temp_c_times_humidity
    FROM enriched_device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 04:00:00'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        reading_ts,
        temp_c,
        status_bits_avg_30m,
        corr_pressure_humidity_avg_30m,
        prev_humidity,
        LAG(corr_pressure_humidity_avg_30m, 1) OVER (ORDER BY prev_humidity, id) AS prev_corr_pressure_humidity_avg_30m,
        LAST_VALUE(status_bits_avg_30m) IGNORE NULLS OVER (ORDER BY corr_pressure_humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status_bits_avg_30m,
        AVG(temp_c_times_humidity) OVER (ORDER BY corr_pressure_humidity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS temp_c_times_humidity_avg_6,
        ROW_NUMBER() OVER (ORDER BY corr_pressure_humidity DESC, id) AS seq_no
    FROM base
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(seq_no) OVER (ORDER BY seq_no, id) AS next_seq_no,
        LAG(seq_no) OVER (ORDER BY seq_no, id) AS prev_seq_no,
        LAG(last_known_status_bits_avg_30m) OVER (ORDER BY seq_no, id) AS prev_last_known_status_bits_avg_30m,
        ROW_NUMBER() OVER (ORDER BY seq_no, id) AS event_seq,
        LEAD(temp_c, 1) OVER (ORDER BY seq_no, id) AS next_temp_c
    FROM scored
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    APPROX_PERCENTILE(next_temp_c, ARRAY(0.25, 0.5, 0.75)) AS next_temp_c_quartiles,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes,
    ROUND(AVG(corr_pressure_humidity_avg_30m), 2) AS avg_corr_pressure_humidity_avg_30m
FROM with_windows
GROUP BY DATE_TRUNC('HOUR', reading_ts)
HAVING ROUND(AVG(corr_pressure_humidity_avg_30m), 2) > 0
ORDER BY hour_window DESC
