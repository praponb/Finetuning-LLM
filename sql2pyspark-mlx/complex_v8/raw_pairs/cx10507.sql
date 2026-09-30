SELECT id, tariff
FROM tariff_windows
WHERE rate <= 5.7
  AND tariff IN ('TOU', 'FLAT')
UNION ALL
SELECT id, site_name
FROM sites
WHERE commissioned_on >= DATE '2024-09-15'
  AND zone IN ('Z2', 'Z3')
