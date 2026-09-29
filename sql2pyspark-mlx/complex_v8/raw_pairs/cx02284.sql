WITH valid AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE quality IN ('EST', 'BAD')
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 60 THEN 1 ELSE 0 END AS is_new_session
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
        SUM(kwh) AS session_kwh,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, session_id
),
derived AS (
    -- Derive helper columns
    SELECT
        site_id,
        session_id,
        session_start,
        session_kwh,
        ROUND(duration_min * 1.1, 2) AS duration_min_adj
    FROM per_session
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.site_id,
        s.session_id,
        s.session_start,
        s.session_kwh,
        s.duration_min_adj,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage
    FROM derived s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    site_id,
    session_id,
    session_start,
    session_kwh,
    duration_min_adj,
    reading_id,
    reading_quality,
    reading_read_at,
    reading_voltage
FROM detail_rows_readings
ORDER BY reading_quality DESC, reading_voltage DESC
