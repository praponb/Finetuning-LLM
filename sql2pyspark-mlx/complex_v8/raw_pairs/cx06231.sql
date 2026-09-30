SELECT
    'site' AS source,
    zone AS name,
    site_name AS name_2,
    commissioned_on
FROM sites
UNION ALL
SELECT
    'site_2' AS source,
    zone AS name,
    site_name AS name_2,
    commissioned_on
FROM sites
WHERE tariff <> 'EV'
ORDER BY name DESC, commissioned_on
