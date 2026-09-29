SELECT id
FROM tariff_windows
WHERE tariff IN ('FLAT', 'TOU')
EXCEPT
SELECT id
FROM tariff_windows
ORDER BY id DESC
