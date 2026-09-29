SELECT
    'site' AS source,
    tariff,
    commissioned_on,
    zone AS name
FROM sites
WHERE zone = 'Z4'
  AND installed_kw BETWEEN 93.3 AND 284.0
UNION ALL
SELECT
    'site_2' AS source,
    tariff,
    commissioned_on,
    site_name
FROM sites
WHERE commissioned_on < DATE '2024-11-21'
  AND installed_kw < 373.3
UNION ALL
SELECT
    'site_3' AS source,
    site_name,
    commissioned_on,
    tariff
FROM sites
WHERE zone IN ('Z2', 'Z3')
ORDER BY commissioned_on DESC, name
