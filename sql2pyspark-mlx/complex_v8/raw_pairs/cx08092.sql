WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN voltage >= 237.5 THEN 'A' WHEN voltage >= 222.6 THEN 'B' ELSE 'C' END AS voltage_tier
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
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
enriched_site AS (
    -- Join sites
    SELECT
        s.site_id,
        s.session_id,
        s.session_end,
        s.event_count,
        s.duration_min,
        si.tariff AS site_tariff,
        si.site_name
    FROM per_session s
    JOIN sites si ON si.id = s.site_id
)
SELECT
    site_id,
    session_end,
    event_count,
    duration_min,
    site_tariff,
    site_name,
    session_id + event_count AS session_id_plus_event_count
FROM enriched_site
ORDER BY event_count, duration_min DESC
