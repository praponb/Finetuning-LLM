WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN voltage >= 235.0 THEN 'large' WHEN voltage >= 222.5 THEN 'medium' ELSE 'small' END AS voltage_tier
    FROM meter_readings
    WHERE kwh >= 42.1
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM recent_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 45 THEN 1 ELSE 0 END AS starts_session
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
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_id
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM sessions
    WHERE duration_min < (SELECT MAX(duration_min) FROM sessions)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(duration_min) OVER (PARTITION BY quality, event_count ORDER BY session_end, quality, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_min,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality, event_count ORDER BY session_end, quality, session_id), 3) AS session_end_percent_rank
    FROM qualified
)
-- Final projection
SELECT
    quality,
    session_end,
    event_count,
    duration_min,
    session_end_percent_rank
FROM with_windows
