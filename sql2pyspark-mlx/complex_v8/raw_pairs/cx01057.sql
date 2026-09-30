WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('WEEK', read_at) AS read_at_week,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND voltage < 235.8
),
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.read_at_week,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
),
enriched AS (
    -- Join sites
    SELECT s.*, si.installed_kw AS site_installed_kw
    FROM with_details_site s
    JOIN sites si ON si.id = s.site_id AND si.commissioned_on BETWEEN DATE '2024-09-15' AND DATE '2025-11-28'
    WHERE si.installed_kw <= 373.3
)
SELECT
    id,
    read_at,
    voltage,
    quality,
    site_commissioned_on
FROM enriched
WHERE HOUR(read_at) BETWEEN 7 AND 13
ORDER BY site_commissioned_on DESC, id
LIMIT 5
