WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM recent_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 120 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        site_id,
        session_no,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(voltage) AS session_voltage,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, session_no
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.site_id,
        s.session_no,
        s.n_events,
        s.duration_min,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM per_session s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.read_at < TIMESTAMP '2025-02-24 10:00:00'
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM line_level_readings
    WHERE duration_min < (SELECT MAX(duration_min) FROM line_level_readings)
)
SELECT
    site_id,
    n_events,
    duration_min,
    reading_voltage,
    reading_kwh,
    reading_quality,
    ROUND(duration_min * 1.1, 2) AS duration_min_adj,
    LENGTH(TRIM(reading_quality)) AS reading_quality_len
FROM qualified
ORDER BY duration_min DESC
