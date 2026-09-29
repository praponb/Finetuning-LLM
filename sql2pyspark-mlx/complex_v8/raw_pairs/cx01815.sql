WITH valid_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM sites
)
SELECT id AS entity_id, commissioned_on AS day, site_name
FROM valid_sites
WHERE tariff = 'FLAT'
  AND site_name IS NOT NULL
UNION ALL
SELECT id, commissioned_on, zone
FROM sites
WHERE installed_kw BETWEEN 96.9 AND 386.6
  AND tariff = 'EV'
UNION ALL
SELECT id, commissioned_on, zone
FROM sites
WHERE tariff = 'FLAT'
  AND zone NOT IN ('Z3', 'Z4')
