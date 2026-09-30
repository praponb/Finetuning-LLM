WITH valid AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        site_id,
        session_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS events,
        SUM(voltage) AS session_voltage,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, session_id
),
joined AS (
    -- Join sites
    SELECT
        s.site_id,
        s.session_id,
        s.session_end,
        s.events,
        s.session_voltage,
        s.duration_min,
        si.tariff AS site_tariff,
        si.zone AS site_zone
    FROM session_summary s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone = 'Z2'
)
-- Final projection
SELECT
    site_id,
    session_id,
    session_end,
    events,
    session_voltage,
    duration_min,
    site_tariff,
    site_zone,
    SUBSTRING(TRIM(site_tariff), 1, 3) AS site_tariff_prefix,
    CONCAT(site_tariff, '#', CAST(site_id AS STRING)) AS site_tariff_ref
FROM joined
ORDER BY site_zone NULLS LAST
