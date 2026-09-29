WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        GREATEST(voltage, kwh) AS greatest_voltage_kwh,
        CASE WHEN voltage >= 233.8 THEN 'A' WHEN voltage >= 224.8 THEN 'B' ELSE 'C' END AS voltage_tier,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM meter_readings
),
with_details_site AS (
    -- Join sites
    SELECT s.*, si.commissioned_on AS site_commissioned_on, si.tariff AS site_tariff
    FROM valid_readings s
    JOIN sites si ON si.id = s.site_id
)
SELECT
    COALESCE(quality, 'ALL') AS quality,
    COALESCE(voltage_tier, 'ALL') AS voltage_tier,
    site_id,
    ROUND(SUM(kwh) / NULLIF(SUM(voltage), 0), 3) AS kwh_to_voltage_ratio,
    PERCENTILE_APPROX(kwh, 0.5) AS median_kwh,
    COUNT(DISTINCT site_tariff) AS distinct_site_tariffs
FROM with_details_site
GROUP BY CUBE(quality, voltage_tier, site_id)
ORDER BY quality NULLS LAST, voltage_tier NULLS LAST, site_id NULLS LAST
