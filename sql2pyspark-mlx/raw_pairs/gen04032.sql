WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        ROUND(status_bits, 1) status_bits_rounded
    FROM device_readings
    WHERE status_bits >= 5
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM base
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        status_bits,
        status_bits_rounded,
        SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 1 HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions_device_readings
),
sessions_device_readings_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps_2
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        reading_ts,
        status_bits_rounded,
        session_start_flag,
        AVG(session_no) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_avg_session_no,
        MIN(session_no) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_min_session_no,
        MAX(session_no) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_max_session_no,
        SUM(session_no) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_sum_session_no,
        LAG(status_bits_rounded, 1) OVER (PARTITION BY DATE_TRUNC('week', reading_ts) ORDER BY island_id, id) prev_status_bits_rounded
    FROM sessions_device_readings_2
    WHERE status_bits <> 11
)
-- Ranking and gap features
SELECT
    *,
    AVG(prev_status_bits_rounded) OVER (PARTITION BY device_code ORDER BY session_start_flag, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) prev_status_bits_rounded_avg_2_prior,
    LEAD(last3_min_session_no, 1) OVER (PARTITION BY TO_DATE(reading_ts) ORDER BY last3_min_session_no, id) next_last3_min_session_no,
    SUM(last3_max_session_no) OVER (ORDER BY last3_min_session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_last3_max_session_no
FROM scored;
