WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT *
    FROM meter_readings
    WHERE quality <> 'EST'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, quality ORDER BY read_at), 3) AS read_at_cume_dist,
        voltage - LAG(voltage) OVER (PARTITION BY site_id, quality ORDER BY read_at, id) AS voltage_change
    FROM cleaned_readings
),
joined_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.read_at_cume_dist,
        si.site_name
    FROM with_windows s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    site_name
FROM joined_site
