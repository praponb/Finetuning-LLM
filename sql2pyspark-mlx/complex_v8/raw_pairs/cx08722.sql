SELECT id, region AS name
FROM policyholders
WHERE segment NOT IN ('SME', 'RETAIL')
  AND joined_on >= DATE '2024-09-04'
INTERSECT
SELECT id, status
FROM policies
WHERE product IN ('LIFE', 'HEALTH', 'TRAVEL')
  AND start_date BETWEEN DATE '2024-10-22' AND DATE '2025-05-30'
ORDER BY id
