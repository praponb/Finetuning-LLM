WITH base_readings AS (
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
    FROM base_readings
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
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(voltage - 225.2) AS voltage_distance,
        DATEDIFF(DATE '2025-06-30', prev_read_at) AS days_since_prev_read_at
    FROM sessionized
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.prev_read_at,
        s.session_id,
        s.voltage_distance,
        s.days_since_prev_read_at,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM annotated s
    JOIN sites si ON si.id = s.site_id AND si.tariff NOT IN ('TOU', 'EV')
    WHERE YEAR(si.commissioned_on) = 2024
)
-- Final projection
SELECT
    id,
    read_at,
    voltage,
    prev_read_at,
    session_id,
    voltage_distance,
    days_since_prev_read_at,
    site_zone,
    site_commissioned_on
FROM enriched_site
ORDER BY read_at DESC
