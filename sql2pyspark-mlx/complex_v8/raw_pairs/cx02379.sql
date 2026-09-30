WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LEAST(kwh, voltage) AS least_kwh_voltage
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        site_id,
        session_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, session_id
),
scored AS (
    -- Label and score rows
    SELECT
        site_id,
        session_id,
        session_end,
        COALESCE(duration_min, 0.0) AS duration_min_filled,
        LEAST(n_events, duration_min) AS least_n_events_duration_min,
        COALESCE(duration_min, 0.0) AS duration_min_filled_2
    FROM per_session
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.site_id,
        s.session_id,
        s.session_end,
        s.duration_min_filled,
        s.least_n_events_duration_min,
        s.duration_min_filled_2,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage
    FROM scored s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT site_id, session_id, field, amount
FROM (
    SELECT site_id, session_id, reading_kwh, least_n_events_duration_min, duration_min_filled_2
    FROM detail_rows_readings
) src
UNPIVOT (
    amount FOR field IN (reading_kwh, least_n_events_duration_min, duration_min_filled_2)
) unp
ORDER BY site_id, session_id, field
