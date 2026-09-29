SELECT id AS ref_id, commissioned_on AS day
FROM sites
WHERE site_name IS NOT NULL
UNION ALL
SELECT id, commissioned_on
FROM sites
WHERE zone = 'Z3'
  AND installed_kw < 373.3
ORDER BY ref_id DESC, day
