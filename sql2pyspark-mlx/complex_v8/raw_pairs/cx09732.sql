SELECT tariff
FROM tariff_windows
INTERSECT ALL
SELECT tariff
FROM sites
WHERE installed_kw BETWEEN 96.9 AND 446.9
  AND tariff IN ('EV', 'TOU')
