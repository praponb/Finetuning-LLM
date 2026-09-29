SELECT id
FROM sites
WHERE tariff IN ('FLAT', 'TOU')
  AND installed_kw < 386.6
EXCEPT ALL
SELECT id
FROM tariff_windows
WHERE MONTH(valid_to) IN (1, 2)
  AND tariff <> 'FLAT'
