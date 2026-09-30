SELECT id, tariff AS label, installed_kw
FROM sites
WHERE installed_kw BETWEEN 96.9 AND 386.6
UNION ALL
SELECT id, tariff, rate
FROM tariff_windows
