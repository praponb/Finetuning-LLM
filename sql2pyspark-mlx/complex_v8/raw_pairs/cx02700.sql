WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage + kwh AS voltage_plus_kwh,
        CASE WHEN quality IN ('BAD', 'OK') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE MONTH(read_at) IN (2, 3)
      AND site_id IS NOT NULL
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        voltage_plus_kwh,
        quality_group,
        ROUND(voltage / NULLIF(voltage_plus_kwh, 0), 3) AS voltage_per_voltage_plus_kwh,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        FLOOR(voltage / 5) * 5 AS voltage_bucket
    FROM recent_readings
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.voltage_plus_kwh,
        s.quality_group,
        s.quality_slug,
        s.voltage_bucket,
        si.tariff AS site_tariff
    FROM tagged s
    LEFT JOIN sites si ON si.id = s.site_id AND si.commissioned_on IS NOT NULL
)
-- Subtotals with ROLLUP
SELECT
    site_tariff,
    quality_group,
    SUM(CASE WHEN site_tariff = 'FLAT' THEN voltage ELSE 0 END) AS voltage_site_tariff_flat,
    COUNT(DISTINCT id) AS distinct_ids
FROM joined
GROUP BY ROLLUP(site_tariff, quality_group)
