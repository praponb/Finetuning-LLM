SELECT id, level
FROM adjusters
WHERE team = 'Property'
UNION
SELECT adjuster_id AS id, policy_id AS level
FROM claims
ORDER BY level
