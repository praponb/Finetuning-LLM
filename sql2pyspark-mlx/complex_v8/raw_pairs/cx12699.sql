WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE MONTH(read_at) IN (1, 2)
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
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
        SUM(kwh) AS session_kwh,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, session_no
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.site_id,
        s.session_no,
        s.session_start,
        s.session_end,
        s.n_events,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality
    FROM per_session s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.voltage < 233.2
)
-- Final projection
SELECT
    site_id,
    session_end,
    n_events,
    reading_read_at,
    reading_voltage,
    DATE_TRUNC('MONTH', session_end) AS session_end_month,
    LOWER(REGEXP_REPLACE(TRIM(reading_quality), ' ', '_')) AS reading_quality_slug
FROM exploded_detail_readings
ORDER BY session_end DESC, reading_voltage DESC
