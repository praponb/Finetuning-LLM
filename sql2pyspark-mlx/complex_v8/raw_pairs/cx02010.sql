SELECT id AS key_id, installed_kw AS amount, 'site' AS record_type
FROM sites
WHERE UPPER(TRIM(site_name)) LIKE 'S%'
UNION ALL
SELECT id, rate, 'tariff' AS record_type
FROM tariff_windows
WHERE tariff = 'FLAT'
