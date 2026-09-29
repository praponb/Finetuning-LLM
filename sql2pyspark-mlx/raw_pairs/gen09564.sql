WITH prev_events AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM device_readings
),
with_gaps AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
with_windows AS (
    SELECT
        *,
        LEAD(pressure) OVER w_part AS next_pressure,
        LAG(temp_c) OVER w_part AS prev_temp_c,
        LEAD(temp_c) OVER w_part AS next_temp_c,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM islands
    WHERE pressure <= 1002
    WINDOW w_part AS (PARTITION BY device_code ORDER BY reading_ts, id)
),
cleaned AS (
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        prev_reading_ts,
        session_start_flag,
        prev_temp_c,
        next_temp_c,
        seq_no,
        temp_c + humidity AS temp_c_plus_humidity
    FROM with_windows
    WHERE next_pressure <= 1023
),
enriched AS (
    SELECT
        *,
        LEAD(status_bits) OVER w_recent AS next_status_bits,
        LAG(status_bits) OVER w_recent AS prev_status_bits,
        LEAD(seq_no) OVER w_recent AS next_seq_no,
        ROW_NUMBER() OVER w_recent AS seq_no_2,
        NTILE(3) OVER (PARTITION BY device_code ORDER BY session_start_flag DESC, id) AS session_start_flag_bucket
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY temp_c_plus_humidity, id)
),
prev_events_2 AS (
    SELECT *, LAG(prev_reading_ts) OVER (PARTITION BY device_code ORDER BY prev_reading_ts, id) AS prev_prev_reading_ts
    FROM enriched
),
gaps AS (
    SELECT *, CASE WHEN prev_prev_reading_ts IS NULL OR prev_reading_ts > prev_prev_reading_ts + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events_2
),
sessionized AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY prev_reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
latest AS (
    SELECT *
    FROM sessionized
    QUALIFY RANK() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS FIRST) = 1
)
SELECT
    DATE_TRUNC('QUARTER', prev_prev_reading_ts) AS quarter_start,
    DATE_TRUNC('YEAR', prev_prev_reading_ts) AS year_start,
    SUM(CASE WHEN device_code = 'DV-04' THEN humidity ELSE 0 END) AS dv_04_humidity,
    MIN(is_new_burst) AS min_is_new_burst,
    COUNT(*) AS event_count,
    MIN(prev_reading_ts) AS first_prev_reading_ts,
    MAX(prev_reading_ts) AS ended_at,
    COUNT_IF(device_code = 'DV-03') AS dv_03_events,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', prev_prev_reading_ts) ORDER BY COUNT_IF(device_code = 'DV-03') DESC) AS dv_03_events_dense_rank
FROM latest
GROUP BY 1, 2
