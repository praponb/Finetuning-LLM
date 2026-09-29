WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATE_TRUNC('QUARTER', read_at) AS read_at_quarter
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM valid_readings
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
        COUNT(*) AS events,
        SUM(kwh) AS session_kwh,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, session_id
),
line_level AS (
    -- One row per reading
    SELECT
        s.site_id,
        s.session_id,
        s.session_end,
        s.session_kwh,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage
    FROM per_session s
    JOIN meter_readings m ON m.site_id = s.site_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(session_kwh, 2) OVER (PARTITION BY TO_DATE(reading_read_at) ORDER BY session_end, site_id, session_id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_session_kwh,
        MAX(session_kwh) OVER (PARTITION BY TO_DATE(reading_read_at) ORDER BY session_end, site_id, session_id, reading_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS session_kwh_max_7
    FROM line_level
)
-- Final projection
SELECT
    site_id,
    session_id,
    session_kwh,
    reading_id,
    reading_read_at,
    reading_voltage,
    second_session_kwh,
    session_kwh_max_7,
    GREATEST(reading_voltage, session_kwh_max_7) AS greatest_reading_voltage_session_kwh_max_7,
    ROUND(session_kwh / NULLIF(session_id, 0), 3) AS session_kwh_per_session_id
FROM ranked
WHERE site_id IS NOT NULL
ORDER BY reading_read_at
