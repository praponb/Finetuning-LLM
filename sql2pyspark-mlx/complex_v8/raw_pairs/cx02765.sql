WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE UPPER(TRIM(site_name)) LIKE 'H%'
)
SELECT id AS entity_id
FROM valid
WHERE commissioned_on < DATE '2024-11-21'
  AND tariff <> 'FLAT'
EXCEPT
SELECT id AS entity_id
FROM tariff_windows
ORDER BY entity_id
