SELECT 'site' AS record_type, id AS entity_id, tariff
FROM sites
WHERE commissioned_on < DATE '2024-11-21'
UNION ALL
SELECT 'tariff' AS record_type, id, tariff
FROM tariff_windows
ORDER BY record_type
