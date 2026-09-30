WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        GREATEST(voltage, kwh) AS greatest_voltage_kwh,
        FLOOR(voltage / 5) * 5 AS voltage_bucket
    FROM meter_readings
    WHERE voltage >= 220.2
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        quality_slug,
        voltage_bucket,
        UPPER(TRIM(quality_slug)) AS quality_slug_clean,
        DAYOFWEEK(read_at) AS read_at_dayofweek,
        GREATEST(voltage_bucket, greatest_voltage_kwh) AS greatest_voltage_bucket_greatest_voltage_kwh
    FROM prepared_readings
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.quality_slug,
        s.quality_slug_clean,
        s.read_at_dayofweek,
        s.greatest_voltage_bucket_greatest_voltage_kwh,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff
    FROM tagged s
    JOIN sites si ON si.id = s.site_id
),
enriched AS (
    -- Add calculated columns
    SELECT *, YEAR(read_at) AS read_at_year
    FROM with_details
    WHERE quality IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, reading
FROM (
    SELECT id, greatest_voltage_bucket_greatest_voltage_kwh, site_installed_kw
    FROM enriched
) src
UNPIVOT (
    reading FOR metric IN (greatest_voltage_bucket_greatest_voltage_kwh, site_installed_kw)
) unp
ORDER BY id, metric
