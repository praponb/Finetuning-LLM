WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE (kwh <= 106.0 OR site_id <= 8)
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM scoped s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.quality NOT IN ('EST', 'BAD')
        )
),
with_activity_2 AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_activity s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage >= 218.5
        )
      AND s.site_id >= 6
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug_2
    FROM with_activity_2
)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, voltage, kwh
    FROM tagged
) src
UNPIVOT (
    metric_value FOR metric_name IN (voltage, kwh)
) unp
