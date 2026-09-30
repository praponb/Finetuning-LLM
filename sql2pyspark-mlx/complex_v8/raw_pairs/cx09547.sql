WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE kwh <= 113.0
      AND quality IN ('BAD', 'OK')
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, si.zone AS site_zone
    FROM recent_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone IN ('Z4', 'Z2')
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        quality,
        ROUND(SUM(kwh_per_voltage) / NULLIF(SUM(voltage), 0), 3) AS kwh_per_voltage_to_voltage_ratio,
        MIN(kwh_per_voltage) AS min_kwh_per_voltage,
        ROUND(SUM(kwh), 2) AS total_kwh,
        COUNT(DISTINCT site_zone) AS distinct_site_zones
    FROM enriched
    GROUP BY quality
    HAVING SUM(kwh) > 35.0
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM per_group
    WHERE min_kwh_per_voltage < (SELECT MAX(min_kwh_per_voltage) FROM per_group)
)
-- Native SQL UNPIVOT (wide to long)
SELECT quality, field, metric_value
FROM (
    SELECT quality, min_kwh_per_voltage, total_kwh, kwh_per_voltage_to_voltage_ratio
    FROM without_matches
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR field IN (min_kwh_per_voltage, total_kwh, kwh_per_voltage_to_voltage_ratio)
) unp
