WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 7 AND 13
),
with_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.quality_ref,
        si.tariff AS site_tariff,
        si.site_name,
        si.installed_kw AS site_installed_kw
    FROM prepared_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff IS NOT NULL
),
screened AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM with_details s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND LENGTH(TRIM(si.site_name)) >= 4
              AND si.commissioned_on < DATE '2025-05-07'
        )
),
with_activity AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM screened s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.zone IS NOT NULL
              AND si.tariff IS NOT NULL
        )
),
derived AS (
    -- Derive helper columns
    SELECT *, COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM with_activity
)
-- Final projection
SELECT
    id,
    read_at,
    quality,
    site_tariff,
    site_name,
    quality_or_unknown
FROM derived
ORDER BY id DESC
LIMIT 10
