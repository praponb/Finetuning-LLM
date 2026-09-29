SELECT id AS key_id, installed_kw
FROM sites
UNION ALL
SELECT id AS key_id, rate AS installed_kw
FROM tariff_windows
WHERE valid_to < TIMESTAMP '2025-03-09 05:00:00'
  AND rate > 2.2
ORDER BY installed_kw DESC, key_id
