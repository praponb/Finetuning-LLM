SELECT id AS key_id
FROM sites
EXCEPT ALL
SELECT id AS key_id
FROM tariff_windows
ORDER BY key_id
