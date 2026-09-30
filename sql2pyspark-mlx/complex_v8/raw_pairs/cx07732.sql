SELECT id AS ref_id
FROM policyholders
INTERSECT ALL
SELECT id
FROM policyholders
WHERE joined_on >= DATE '2024-06-18'
  AND region NOT IN ('East', 'Central')
ORDER BY ref_id DESC
