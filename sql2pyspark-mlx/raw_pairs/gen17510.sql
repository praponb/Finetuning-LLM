WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(status_bits / pressure, 2) AS status_bits_per_pressure
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits_per_pressure DESC NULLS LAST, id) = 1
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM ranked
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        status_bits_per_pressure,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM sessionized
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM gaps
),
ranked_device_readings AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts DESC, id) AS row_num
        FROM islands
    ) x
    WHERE row_num = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        status_bits,
        status_bits_per_pressure,
        island_id,
        island_id_2,
        row_num,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY status_bits DESC) AS status_bits_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY island_id_2) AS island_id_2_percent_rank,
        PERCENT_RANK() OVER (ORDER BY status_bits_per_pressure) AS status_bits_per_pressure_percent_rank,
        LEAD(island_id, 2, 0) OVER (PARTITION BY TO_DATE(reading_ts), device_code ORDER BY temp_c, id) AS next_island_id
    FROM ranked_device_readings
)
-- Ranking and gap features
SELECT
    id,
    status_bits,
    status_bits_per_pressure,
    island_id_2,
    row_num,
    status_bits_per_pressure_percent_rank,
    next_island_id,
    ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits_dense_rank, id) AS row_seq,
    AVG(island_id_2) OVER (PARTITION BY device_code ORDER BY island_id, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS island_id_2_avg_2_prior,
    SUM(temp_c) OVER (PARTITION BY device_code ORDER BY island_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_temp_c,
    MIN(status_bits_per_pressure) OVER w_recent AS device_code_min_status_bits_per_pressure
FROM metrics
WHERE status_bits_per_pressure < 25
WINDOW w_recent AS (PARTITION BY device_code)
