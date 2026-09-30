SELECT
    id,
    rate,
    tariff,
    'tariff' AS source
FROM tariff_windows
UNION ALL
SELECT
    id,
    installed_kw,
    site_name,
    'site' AS source
FROM sites
WHERE zone <> 'Z3'
  AND commissioned_on BETWEEN DATE '2024-06-27' AND DATE '2025-05-10'
