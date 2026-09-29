WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN pressure >= 1019 THEN 'High' WHEN pressure >= 990 THEN 'Medium' ELSE 'Low' END AS pressure_tier
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 03:00:00'
      AND reading_ts >= '2025-09-20'
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) AS prev_reading_ts
    FROM valid
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN island_id >= 9 THEN 'LARGE' ELSE 'SMALL' END AS island_id_tier,
    APPROX_PERCENTILE(island_id, ARRAY(0.25, 0.5, 0.75)) AS island_id_quartiles,
    PERCENTILE_APPROX(pressure, ARRAY(0.25, 0.5, 0.75)) AS pressure_quartiles
FROM sessions_device_readings
GROUP BY CASE WHEN island_id >= 9 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY island_id_tier DESC;
