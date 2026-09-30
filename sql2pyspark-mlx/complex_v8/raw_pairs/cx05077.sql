WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality_or_unknown
    SELECT *, LAG(read_at) OVER (PARTITION BY quality_or_unknown ORDER BY read_at, id) AS prev_read_at
    FROM valid_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY quality_or_unknown ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        quality_or_unknown,
        session_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(voltage) AS session_voltage,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality_or_unknown, session_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT session_end, session_id, quality_or_unknown, duration_min
    FROM sessions
) src
PIVOT (
    AVG(duration_min) AS avg_v, COUNT(duration_min) AS n
    FOR quality_or_unknown IN ('EST', 'UNKNOWN', 'OK')
)
ORDER BY session_end, session_id
