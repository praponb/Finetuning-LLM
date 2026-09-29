SELECT
    'site' AS source,
    id,
    commissioned_on,
    installed_kw AS metric
FROM sites
UNION
SELECT
    'site_2' AS source,
    id,
    commissioned_on,
    installed_kw
FROM sites
WHERE LENGTH(TRIM(site_name)) >= 5
  AND zone IN ('Z2', 'Z4')
ORDER BY source DESC, metric
