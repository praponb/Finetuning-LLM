SELECT id AS ref_id, commissioned_on AS day
FROM sites
WHERE zone IN ('Z3', 'Z2')
  AND LENGTH(TRIM(site_name)) >= 7
UNION ALL
SELECT id AS ref_id, commissioned_on AS day
FROM sites
UNION ALL
SELECT id AS ref_id, commissioned_on AS day
FROM sites
