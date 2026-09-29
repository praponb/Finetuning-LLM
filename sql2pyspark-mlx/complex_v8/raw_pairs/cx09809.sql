WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour
    FROM meter_readings
    WHERE MONTH(read_at) IN (1, 2)
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM valid_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 120 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
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
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM sessions
    WHERE session_id > (SELECT AVG(session_id) FROM sessions)
)
SELECT
    quality,
    session_start,
    session_end,
    event_count,
    LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
    QUARTER(session_start) AS session_start_quarter
FROM with_activity
ORDER BY session_start_quarter DESC, quality_slug DESC
