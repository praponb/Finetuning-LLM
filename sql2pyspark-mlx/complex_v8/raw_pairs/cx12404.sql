SELECT
    id AS entity_id,
    site_name AS name,
    commissioned_on,
    'site' AS record_type
FROM sites
UNION ALL
SELECT
    id,
    zone,
    commissioned_on,
    'site_2' AS record_type
FROM sites
UNION ALL
SELECT
    id,
    zone,
    commissioned_on,
    'site_3' AS record_type
FROM sites
WHERE YEAR(commissioned_on) = 2024
ORDER BY entity_id DESC
