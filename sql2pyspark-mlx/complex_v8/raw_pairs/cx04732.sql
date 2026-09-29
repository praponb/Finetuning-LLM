WITH recent_sites AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        commissioned_on
    FROM sites
    WHERE zone NOT IN ('Z3', 'Z2')
      AND tariff = 'EV'
)
SELECT 'site' AS source, zone, site_name AS label
FROM recent_sites
WHERE commissioned_on < DATE '2025-05-10'
  AND UPPER(TRIM(site_name)) LIKE 'D%'
UNION ALL
SELECT 'site_2' AS source, tariff, site_name
FROM sites
WHERE installed_kw BETWEEN 96.9 AND 284.0
UNION ALL
SELECT 'site_3' AS source, site_name, zone
FROM sites
WHERE tariff IN ('EV', 'FLAT')
  AND UPPER(TRIM(site_name)) LIKE 'R%'
