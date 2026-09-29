WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LEAST(voltage, kwh) AS least_voltage_kwh,
        LENGTH(TRIM(quality)) AS quality_len,
        GREATEST(voltage, kwh) AS greatest_voltage_kwh
    FROM meter_readings
    WHERE kwh > 52.4
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at,
        COALESCE(voltage, 0.0) AS voltage_filled,
        FLOOR(greatest_voltage_kwh / 50) * 50 AS greatest_voltage_kwh_bucket
    FROM valid_readings
    WHERE kwh >= 45.0
),
screened AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM tagged s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.installed_kw >= 141.6
        )
),
with_details AS (
    -- Join sites
    SELECT s.*, si.zone AS site_zone
    FROM screened s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality_len,
    greatest_voltage_kwh,
    days_since_read_at,
    GREATEST(voltage_filled, days_since_read_at) AS greatest_voltage_filled_days_since_read_at
FROM with_details
ORDER BY days_since_read_at NULLS LAST
