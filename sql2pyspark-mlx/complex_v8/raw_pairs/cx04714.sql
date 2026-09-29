SELECT id AS entity_id, region AS category
FROM policyholders
UNION ALL
SELECT holder_id, status
FROM policies
UNION ALL
SELECT id, segment
FROM policyholders
WHERE segment IN ('VIP', 'SME')
ORDER BY category
