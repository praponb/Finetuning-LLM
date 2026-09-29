WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality_or_unknown
    SELECT *, LAG(read_at) OVER (PARTITION BY quality_or_unknown ORDER BY read_at, id) AS prev_read_at
    FROM recent_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY quality_or_unknown ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        quality_or_unknown,
        session_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS events,
        SUM(kwh) AS session_kwh,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality_or_unknown, session_id
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_session
    WHERE duration_min > (SELECT AVG(duration_min) FROM per_session)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM screened
    WHERE session_id > (SELECT AVG(session_id) FROM screened)
)
-- Subtotals with ROLLUP
SELECT
    events,
    quality_or_unknown,
    session_id,
    COUNT(DISTINCT quality_or_unknown) AS distinct_quality_or_unknowns
FROM with_activity
GROUP BY ROLLUP(events, quality_or_unknown, session_id)
ORDER BY events NULLS LAST, quality_or_unknown NULLS LAST, session_id NULLS LAST
