SELECT
    'site' AS record_type,
    id AS entity_id,
    commissioned_on,
    site_name
FROM sites
WHERE YEAR(commissioned_on) = 2024
  AND tariff NOT IN ('FLAT', 'TOU')
UNION ALL
SELECT
    'site_2' AS record_type,
    id,
    commissioned_on,
    zone
FROM sites
ORDER BY commissioned_on DESC
