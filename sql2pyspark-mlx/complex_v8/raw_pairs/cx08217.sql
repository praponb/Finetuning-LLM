WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        kwh + voltage AS kwh_plus_voltage,
        FLOOR(kwh / 25) * 25 AS kwh_bucket,
        GREATEST(voltage, kwh) AS greatest_voltage_kwh
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-19 09:00:00'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label
    FROM filtered
),
combined_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.kwh_plus_voltage,
        s.kwh_bucket,
        s.greatest_voltage_kwh,
        s.quality_slug,
        s.quality_prefix,
        s.read_at_label,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on,
        si.site_name
    FROM labeled s
    JOIN sites si ON si.id = s.site_id
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        voltage,
        kwh_bucket,
        greatest_voltage_kwh,
        quality_slug,
        quality_prefix,
        site_installed_kw,
        site_commissioned_on,
        kwh_bucket + greatest_voltage_kwh AS kwh_bucket_plus_greatest_voltage_kwh,
        kwh_bucket - greatest_voltage_kwh AS kwh_bucket_minus_greatest_voltage_kwh
    FROM combined_details
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, kwh_bucket_minus_greatest_voltage_kwh, voltage, site_installed_kw
    FROM derived
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR measure IN (kwh_bucket_minus_greatest_voltage_kwh, voltage, site_installed_kw)
) unp
