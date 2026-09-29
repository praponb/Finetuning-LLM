WITH valid_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE installed_kw BETWEEN 66.5 AND 446.9
)
SELECT id, zone, 'site' AS source
FROM valid_sites
UNION ALL
SELECT site_id AS id, quality AS zone, 'reading' AS source
FROM meter_readings
WHERE kwh > 37.0
  AND site_id <= 7
