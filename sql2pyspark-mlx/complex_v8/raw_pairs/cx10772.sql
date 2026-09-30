SELECT id AS entity_id, commissioned_on AS event_date, 'site' AS record_type
FROM sites
WHERE installed_kw < 284.0
UNION ALL
SELECT id, commissioned_on, 'site_2' AS record_type
FROM sites
WHERE zone NOT IN ('Z2', 'Z4')
  AND installed_kw < 284.0
