SELECT policy_id AS entity_id
FROM claims
MINUS
SELECT id
FROM adjusters
ORDER BY entity_id DESC
