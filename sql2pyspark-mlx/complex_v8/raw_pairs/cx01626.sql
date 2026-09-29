WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 45 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.is_new_session,
        s.session_no,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality
    FROM sessionized s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.quality <> 'OK'
),
totals AS (
    -- Group-level totals
    SELECT
        is_new_session,
        SUM(reading_voltage) AS total_reading_voltage,
        ROUND(AVG(is_new_session), 2) AS avg_is_new_session
    FROM line_level_readings
    GROUP BY is_new_session
)
-- Final projection
SELECT
    is_new_session,
    total_reading_voltage,
    total_reading_voltage + is_new_session AS total_reading_voltage_plus_is_new_session,
    ROUND(avg_is_new_session / NULLIF(is_new_session, 0), 3) AS avg_is_new_session_per_is_new_session
FROM totals
WHERE avg_is_new_session >= 0.1
ORDER BY total_reading_voltage_plus_is_new_session DESC, avg_is_new_session_per_is_new_session
