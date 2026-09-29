WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        FLOOR(voltage / 5) * 5 AS voltage_bucket
    FROM meter_readings
    WHERE kwh <= 86.0
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM valid_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        quality,
        session_no,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_no
)
SELECT
    quality,
    session_no,
    n_events,
    duration_min,
    LENGTH(TRIM(quality)) AS quality_len,
    LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
FROM sessions
ORDER BY quality_slug
