SELECT id AS entity_id, commissioned_on, 'site' AS source
FROM sites
UNION ALL
SELECT id, commissioned_on, 'site_2' AS source
FROM sites
WHERE zone IN ('Z4', 'Z3')
UNION ALL
SELECT id, commissioned_on, 'site_3' AS source
FROM sites
WHERE zone = 'Z3'
  AND commissioned_on IS NOT NULL
ORDER BY entity_id, source
