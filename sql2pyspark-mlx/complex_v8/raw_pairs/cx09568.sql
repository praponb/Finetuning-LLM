SELECT
    id AS ref_id,
    site_name AS label,
    commissioned_on AS event_date,
    'site' AS source
FROM sites
UNION ALL
SELECT
    id AS ref_id,
    zone AS label,
    commissioned_on AS event_date,
    'site_2' AS source
FROM sites
WHERE zone IS NOT NULL
UNION ALL
SELECT
    id AS ref_id,
    tariff AS label,
    commissioned_on AS event_date,
    'site_3' AS source
FROM sites
ORDER BY ref_id, event_date
