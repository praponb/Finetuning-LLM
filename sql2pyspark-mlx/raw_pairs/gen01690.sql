WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        status_bits * pressure status_bits_times_pressure
    FROM device_readings
    WHERE reading_ts >= TO_DATE('2025-09-01')
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        status_bits,
        firmware,
        is_new_session,
        LAST_VALUE(humidity, TRUE) OVER w_part last_known_humidity,
        LAST_VALUE(temp_c) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_temp_c,
        LAG(status_bits, 1) OVER w_running prev_status_bits
    FROM sessionized
    WINDOW w_part AS (PARTITION BY device_code ORDER BY status_bits_times_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY device_code ORDER BY status_bits_times_pressure, id)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        status_bits,
        firmware,
        is_new_session,
        last_known_humidity,
        prev_status_bits,
        ROUND(status_bits, 0) status_bits_rounded
    FROM with_windows
    WHERE reading_ts > TIMESTAMP '2025-09-01 03:00:00'
),
with_windows_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(prev_status_bits) OVER (PARTITION BY device_code ORDER BY last_known_humidity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_avg_prev_status_bits,
        SUM(prev_status_bits) OVER (PARTITION BY device_code ORDER BY last_known_humidity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_sum_prev_status_bits,
        MAX(prev_status_bits) OVER (PARTITION BY device_code ORDER BY last_known_humidity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_max_prev_status_bits,
        MIN(prev_status_bits) OVER (PARTITION BY device_code) device_code_min_prev_status_bits
    FROM valid
    WHERE is_new_session BETWEEN 0 AND 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        device_code,
        last7_sum_prev_status_bits,
        last_known_humidity,
        status_bits_rounded
    FROM with_windows_device_readings
) src
UNPIVOT INCLUDE NULLS (
    val FOR kpi IN (last7_sum_prev_status_bits, last_known_humidity, status_bits_rounded)
);
