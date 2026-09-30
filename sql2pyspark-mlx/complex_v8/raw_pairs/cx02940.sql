SELECT id
FROM sites
WHERE UPPER(TRIM(site_name)) LIKE 'S%'
  AND zone IN ('Z2', 'Z3')
EXCEPT
SELECT id
FROM sites
ORDER BY id
