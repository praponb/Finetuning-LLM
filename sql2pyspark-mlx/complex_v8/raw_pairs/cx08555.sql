WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 120 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        quality,
        session_no,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS events,
        SUM(days_since_read_at) AS session_days_since_read_at,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_no
)
-- Final projection
SELECT
    quality,
    session_start,
    session_end,
    events,
    session_days_since_read_at,
    duration_min,
    CASE WHEN session_no >= 4 THEN 'large' WHEN session_no >= 2 THEN 'medium' ELSE 'small' END AS session_no_tier
FROM sessions
WHERE session_no >= 2
ORDER BY session_end DESC, events DESC NULLS FIRST
