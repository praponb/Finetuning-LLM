WITH valid_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE installed_kw < 386.6
)
SELECT id, tariff
FROM valid_sites
WHERE installed_kw <= 373.3
INTERSECT
SELECT id, quality AS tariff
FROM meter_readings
ORDER BY tariff, id
