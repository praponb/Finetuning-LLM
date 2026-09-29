SELECT id AS entity_id, installed_kw, commissioned_on
FROM sites
UNION
SELECT id, installed_kw, commissioned_on
FROM sites
WHERE tariff IS NOT NULL
ORDER BY commissioned_on DESC
