WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        TO_DATE(read_at) AS read_at_date,
        HOUR(read_at) AS read_at_hour,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        si.zone AS site_zone
    FROM recent_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
tagged AS (
    -- Derive helper columns
    SELECT *, DATE_TRUNC('HOUR', read_at) AS read_at_hour
    FROM with_details
    WHERE voltage > 222.6
)
-- Subtotals with GROUPING SETS
SELECT
    quality,
    site_zone,
    site_id,
    GROUPING(quality) AS is_total_quality,
    MAX(read_at_hour) AS last_read_at_hour
FROM tagged
GROUP BY GROUPING SETS ((quality, site_zone, site_id), (quality), ())
ORDER BY quality NULLS LAST, site_zone NULLS LAST, site_id NULLS LAST
