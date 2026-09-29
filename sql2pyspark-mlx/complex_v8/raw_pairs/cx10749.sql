SELECT holder_id AS entity_id, status
FROM policies
WHERE holder_id IS NOT NULL
  AND start_date BETWEEN DATE '2024-08-27' AND DATE '2025-03-23'
UNION ALL
SELECT id AS entity_id, holder_name AS status
FROM policyholders
ORDER BY status
