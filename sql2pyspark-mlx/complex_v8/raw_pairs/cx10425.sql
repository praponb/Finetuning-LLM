WITH valid_readings AS (
    -- Filter meter_readings
    SELECT *
    FROM meter_readings
    WHERE site_id IS NOT NULL
)
SELECT quality AS name, site_id AS entity_id, 'reading' AS source
FROM valid_readings
UNION ALL
SELECT tariff AS name, id AS entity_id, 'site' AS source
FROM sites
UNION ALL
SELECT zone AS name, id AS entity_id, 'site' AS source
FROM sites
WHERE LENGTH(TRIM(site_name)) >= 4
  AND YEAR(commissioned_on) = 2024
ORDER BY source
