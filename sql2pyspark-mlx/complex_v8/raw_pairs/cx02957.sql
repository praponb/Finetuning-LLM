SELECT id
FROM sites
EXCEPT
SELECT id
FROM tariff_windows
WHERE rate < 5.5
ORDER BY id DESC
