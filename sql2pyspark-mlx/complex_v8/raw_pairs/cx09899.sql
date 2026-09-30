WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM prepared_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        quality,
        session_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS events,
        SUM(voltage) AS session_voltage,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_id
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM sessions
    WHERE events > (SELECT AVG(events) FROM sessions)
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_activity
    WHERE duration_min > (SELECT AVG(duration_min) FROM with_activity)
)
-- Final projection
SELECT
    quality,
    session_id,
    session_start,
    session_end,
    events
FROM qualified
ORDER BY quality DESC
