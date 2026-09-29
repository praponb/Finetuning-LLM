WITH base_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE (commissioned_on BETWEEN DATE '2024-09-15' AND DATE '2025-12-03' OR zone = 'Z3')
)
SELECT tariff, site_name AS name
FROM sites
WHERE tariff = 'TOU'
  AND zone NOT IN ('Z3', 'Z2')
UNION ALL
SELECT zone AS tariff, site_name AS name
FROM sites
WHERE tariff IS NOT NULL
