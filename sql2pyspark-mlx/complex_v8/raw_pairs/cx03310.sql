WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        UPPER(TRIM(quality)) AS quality_clean,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE kwh >= 45.3
      AND voltage < 234.7
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.quality_ref,
        si.tariff AS site_tariff
    FROM scoped_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.site_tariff,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at
    FROM with_parent_site s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
-- Subtotals with ROLLUP
SELECT
    site_tariff,
    quality,
    COUNT(*) FILTER (WHERE reading_quality = 'OK') AS n_reading_quality_ok,
    SUM(reading_voltage) AS total_reading_voltage
FROM detail_rows_readings
GROUP BY ROLLUP(site_tariff, quality)
ORDER BY site_tariff NULLS LAST, quality NULLS LAST
