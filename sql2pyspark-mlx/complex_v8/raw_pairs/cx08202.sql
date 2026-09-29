WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        CASE WHEN voltage >= 229.1 THEN 'A' WHEN voltage >= 218.4 THEN 'B' ELSE 'C' END AS voltage_tier,
        LENGTH(TRIM(quality)) AS quality_len,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE kwh >= 45.3
),
with_parent AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.voltage_tier,
        s.quality_ref,
        si.site_name,
        si.installed_kw AS site_installed_kw
    FROM prepared s
    JOIN sites si ON si.id = s.site_id
)
-- Subtotals with CUBE
SELECT
    site_id,
    COALESCE(voltage_tier, 'ALL') AS voltage_tier,
    ROUND(SUM(site_installed_kw) / NULLIF(SUM(voltage), 0), 3) AS site_installed_kw_to_voltage_ratio,
    COUNT(*) AS n_rows,
    MAX(voltage) AS max_voltage
FROM with_parent
GROUP BY CUBE(site_id, voltage_tier)
ORDER BY site_id NULLS LAST, voltage_tier NULLS LAST
