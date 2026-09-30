SELECT id AS ref_id
FROM sites
EXCEPT
SELECT id
FROM tariff_windows
WHERE tariff IN ('TOU', 'EV')
ORDER BY ref_id
