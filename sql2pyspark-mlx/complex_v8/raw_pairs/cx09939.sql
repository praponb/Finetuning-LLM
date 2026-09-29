WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        ABS(kwh - 68.1) AS kwh_distance,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage_2
    FROM meter_readings
    WHERE kwh BETWEEN 20.3 AND 81.3
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM scoped s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.read_at < TIMESTAMP '2025-02-18 10:00:00'
              AND m.voltage <= 229.1
        )
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        voltage,
        quality,
        kwh_distance,
        kwh_per_voltage,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM qualified
    WHERE kwh_distance <= 78.0
),
qualified_2 AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM labeled s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
)
SELECT
    id,
    site_id,
    kwh_distance,
    kwh_per_voltage,
    quality_or_unknown,
    quality_slug,
    COALESCE(quality_or_unknown, 'UNKNOWN') AS quality_or_unknown_or_unknown
FROM qualified_2
ORDER BY kwh_per_voltage DESC, site_id DESC
