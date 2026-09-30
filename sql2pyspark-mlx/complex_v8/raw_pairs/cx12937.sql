SELECT id, valid_to, tariff AS category
FROM tariff_windows
WHERE tariff IN ('FLAT', 'EV')
UNION ALL
SELECT id, valid_from, tariff
FROM tariff_windows
WHERE tariff IS NOT NULL
  AND rate >= 2.2
