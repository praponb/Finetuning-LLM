SELECT policy_id AS entity_id, adjuster_id AS key_id, status AS category
FROM claims
UNION ALL
SELECT level AS entity_id, id AS key_id, team AS category
FROM adjusters
ORDER BY entity_id DESC
