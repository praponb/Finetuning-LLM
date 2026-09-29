SELECT id AS entity_id, policy_id, 'claim' AS src
FROM claims
UNION ALL
SELECT holder_id AS entity_id, id AS policy_id, 'policy' AS src
FROM policies
ORDER BY policy_id
