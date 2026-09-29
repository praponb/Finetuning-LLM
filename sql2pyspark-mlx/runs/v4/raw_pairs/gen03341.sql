WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        pressure,
        status_bits,
        firmware,
        humidity + temp_c AS humidity_plus_temp_c
    FROM device_readings
    WHERE reading_ts > TO_DATE('2025-09-01')
      AND device_code IN ('DV-04', 'DV-03')
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM filtered
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(humidity_plus_temp_c) OVER (PARTITION BY device_code ORDER BY prev_reading_ts, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS humidity_plus_temp_c_avg_2_prior,
        LAG(pressure, 1) OVER (ORDER BY is_new_burst, id) AS prev_pressure,
        MAX(island_id) OVER (PARTITION BY device_code ORDER BY CAST(prev_reading_ts AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS island_id_max_30d,
        LEAD(island_id, 1) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS next_island_id
    FROM islands
)
-- Multi-level aggregation
SELECT
    CASE WHEN island_id >= 9 THEN 'LARGE' ELSE 'SMALL' END AS island_id_tier,
    DATE_TRUNC('QUARTER', prev_reading_ts) AS quarter_start,
    device_code,
    SUM(pressure) FILTER (WHERE device_code = 'DV-01') AS dv_01_total_pressure,
    COUNT_IF(device_code = 'DV-01') AS dv_01_events
FROM scored
GROUP BY CASE WHEN island_id >= 9 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', prev_reading_ts), device_code
HAVING SUM(pressure) FILTER (WHERE device_code = 'DV-01') < 0
ORDER BY dv_01_events DESC;
