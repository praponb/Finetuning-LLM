WITH valid AS (
    SELECT *, CEIL(pressure) AS pressure_whole
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 04:00:00'
),
metrics AS (
    SELECT
        *,
        ROUND(pressure / SUM(pressure) OVER (PARTITION BY device_code), 4) AS pressure_share,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_bits_avg_6_prior,
        FIRST_VALUE(pressure, TRUE) OVER (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_pressure
    FROM valid
),
gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM metrics
),
sessions_device_readings AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        status_bits_avg_6_prior,
        first_known_pressure,
        SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
valid_device_readings AS (
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        status_bits_avg_6_prior,
        first_known_pressure,
        burst_id,
        HOUR(reading_ts) AS reading_ts_hour,
        temp_c * burst_id AS temp_c_times_burst_id
    FROM sessions_device_readings
    WHERE burst_id <= 5
),
scored AS (
    SELECT
        id,
        reading_ts,
        status_bits_avg_6_prior,
        first_known_pressure,
        burst_id,
        temp_c_times_burst_id,
        LAG(first_known_pressure) OVER w_hist AS prev_first_known_pressure,
        LAG(humidity) OVER w_hist AS prev_humidity,
        ROW_NUMBER() OVER w_hist AS row_seq,
        NTILE(4) OVER (PARTITION BY YEAR(reading_ts), device_code ORDER BY first_known_pressure DESC, id) AS first_known_pressure_bucket
    FROM valid_device_readings
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY status_bits_avg_6_prior, id)
)
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    CASE WHEN first_known_pressure_bucket >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS first_known_pressure_bucket_tier,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes,
    APPROX_PERCENTILE(prev_humidity, ARRAY(0.25, 0.5, 0.75)) AS prev_humidity_quartiles,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes_2
FROM scored
GROUP BY hour_window, first_known_pressure_bucket_tier, activity_day
ORDER BY activity_day;
