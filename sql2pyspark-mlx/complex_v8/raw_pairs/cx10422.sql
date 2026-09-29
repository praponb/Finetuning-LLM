WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ABS(kwh - 63.4) AS kwh_distance,
        YEAR(read_at) AS read_at_year
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-02-12'
      AND kwh >= 52.4
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM recent_readings s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND HOUR(m.read_at) BETWEEN 10 AND 13
              AND m.quality IN ('EST', 'BAD')
        )
),
with_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.kwh_distance,
        s.read_at_year,
        si.zone AS site_zone,
        si.site_name
    FROM qualified s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT
    id,
    site_id,
    read_at,
    voltage,
    quality,
    kwh_distance,
    read_at_year,
    site_zone,
    site_name
FROM with_details
ORDER BY voltage DESC
