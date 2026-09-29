WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        ABS(kwh - 37.1) AS kwh_distance
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM prepared_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MAX(read_at) AS last_reading_read_at,
        MAX(read_at) AS last_reading_read_at_2
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at,
        m.last_reading_read_at_2
    FROM sessionized s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
)
SELECT
    id,
    site_id,
    read_at,
    voltage,
    quality,
    kwh_distance,
    prev_read_at,
    last_reading_read_at
FROM with_activity
ORDER BY prev_read_at DESC, read_at DESC
