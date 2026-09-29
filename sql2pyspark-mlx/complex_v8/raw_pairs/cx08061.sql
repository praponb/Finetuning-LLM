SELECT site_name AS name, commissioned_on, 'site' AS src
FROM sites
WHERE installed_kw > 96.9
  AND commissioned_on BETWEEN DATE '2024-06-27' AND DATE '2025-05-10'
UNION ALL
SELECT zone, commissioned_on, 'site_2' AS src
FROM sites
UNION ALL
SELECT tariff, commissioned_on, 'site_3' AS src
FROM sites
ORDER BY src DESC
