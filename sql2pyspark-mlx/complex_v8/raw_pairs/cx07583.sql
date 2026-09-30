SELECT holder_name, id, 'holder' AS source
FROM policyholders
WHERE joined_on < DATE '2025-09-02'
UNION
SELECT status AS holder_name, holder_id AS id, 'policy' AS source
FROM policies
WHERE product = 'HEALTH'
  AND holder_id IS NOT NULL
ORDER BY holder_name
