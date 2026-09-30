SELECT id AS key_id, commissioned_on, 'site' AS origin
FROM sites
UNION
SELECT id, commissioned_on, 'site_2' AS origin
FROM sites
ORDER BY key_id, origin
