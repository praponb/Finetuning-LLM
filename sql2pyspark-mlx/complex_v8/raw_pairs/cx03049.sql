SELECT id, commissioned_on, 'site' AS src
FROM sites
UNION ALL
SELECT id, commissioned_on, 'site_2' AS src
FROM sites
WHERE installed_kw <= 386.6
  AND zone = 'Z2'
UNION ALL
SELECT id, commissioned_on, 'site_3' AS src
FROM sites
WHERE UPPER(TRIM(site_name)) LIKE 'H%'
