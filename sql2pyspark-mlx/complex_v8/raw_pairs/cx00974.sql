WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM meter_readings
    WHERE voltage BETWEEN 220.2 AND 243.1
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM base_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        quality,
        session_no,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(voltage) AS session_voltage,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_no
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM per_session
    WHERE n_events > (SELECT MIN(n_events) FROM per_session)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(session_no, 0) AS session_no_filled,
        ROUND(duration_min * 1.07, 2) AS duration_min_adj
    FROM with_activity
)
SELECT *
FROM (
    SELECT session_start, quality, n_events
    FROM labeled
) src
PIVOT (
    SUM(n_events) AS total, COUNT(n_events) AS n
    FOR quality IN ('BAD', 'OK', 'EST')
)
