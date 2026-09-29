SELECT id AS key_id, commissioned_on
FROM sites
UNION ALL
SELECT id, commissioned_on
FROM sites
WHERE YEAR(commissioned_on) = 2025
UNION ALL
SELECT id, commissioned_on
FROM sites
WHERE tariff IS NOT NULL
ORDER BY key_id DESC, commissioned_on
