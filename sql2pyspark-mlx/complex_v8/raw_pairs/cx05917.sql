WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage BETWEEN 214.3 AND 236.7
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        kwh,
        voltage,
        quality,
        MIN(voltage) OVER w_site_id AS site_id_min_voltage,
        NTILE(5) OVER w_all_by_voltage AS voltage_quintile
    FROM recent_readings
    WINDOW w_site_id AS (PARTITION BY site_id), w_all_by_voltage AS (ORDER BY voltage DESC, id)
)
-- Final projection
SELECT
    id,
    quality,
    voltage_quintile,
    LEAST(kwh, site_id_min_voltage) AS least_kwh_site_id_min_voltage
FROM ranked
WHERE quality <> 'EST'
ORDER BY quality, id DESC NULLS FIRST
