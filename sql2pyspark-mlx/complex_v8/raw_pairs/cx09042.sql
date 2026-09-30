WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM recent_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        site_id,
        session_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(voltage) AS session_voltage,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, session_id
),
with_children AS (
    -- One row per reading
    SELECT
        s.site_id,
        s.session_id,
        s.session_start,
        s.session_voltage,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM sessions s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        site_id,
        session_id,
        session_start,
        reading_id,
        reading_read_at,
        reading_quality,
        COALESCE(session_id, 0) AS session_id_filled,
        SUBSTRING(TRIM(reading_quality), 1, 3) AS reading_quality_prefix
    FROM with_children
)
-- Final projection
SELECT
    site_id,
    session_id,
    session_start,
    reading_id,
    reading_read_at,
    reading_quality,
    session_id_filled,
    reading_quality_prefix,
    SUBSTRING(TRIM(reading_quality), 1, 2) AS reading_quality_prefix_2,
    ABS(session_id_filled - 4) AS session_id_filled_distance
FROM tagged
ORDER BY session_id_filled_distance NULLS LAST, session_id DESC
