WITH valid AS (
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        humidity - temp_c AS humidity_minus_temp_c
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
enriched AS (
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        status_bits,
        NTILE(5) OVER (PARTITION BY device_code ORDER BY status_bits ASC, id ASC) AS status_bits_bucket,
        AVG(status_bits) OVER (ORDER BY reading_ts ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_bits_avg_6_prior,
        NTILE(4) OVER w_grp AS status_bits_bucket_2,
        NTILE(3) OVER (PARTITION BY device_code ORDER BY humidity ASC, id ASC) AS humidity_bucket
    FROM valid
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY status_bits DESC, id ASC)
),
with_gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) > INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM enriched
),
sessions_device_readings AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
prev_events AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) AS prev_reading_ts
    FROM sessions_device_readings
),
with_gaps_2 AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessions_device_readings_2 AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM with_gaps_2
),
enriched_device_readings AS (
    SELECT
        *,
        AVG(status_bits_bucket_2) OVER w_recent AS last3_avg_status_bits_bucket_2,
        COUNT(status_bits_bucket_2) OVER w_recent AS last3_count_status_bits_bucket_2,
        MIN(status_bits_bucket_2) OVER w_recent AS last3_min_status_bits_bucket_2
    FROM sessions_device_readings_2
    WHERE session_start_flag < 0
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY prev_reading_ts ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    humidity,
    status_bits_avg_6_prior,
    status_bits_bucket_2,
    humidity_bucket,
    is_new_session,
    burst_id,
    session_start_flag,
    burst_id_2
FROM enriched_device_readings
ORDER BY status_bits_avg_6_prior ASC;
