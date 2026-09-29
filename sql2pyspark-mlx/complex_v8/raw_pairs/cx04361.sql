SELECT id AS entity_id, commissioned_on
FROM sites
WHERE zone IN ('Z3', 'Z4')
  AND installed_kw <= 373.3
UNION ALL
SELECT id AS entity_id, commissioned_on
FROM sites
