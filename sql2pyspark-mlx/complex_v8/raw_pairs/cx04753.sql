SELECT adjuster_id
FROM claims
EXCEPT
SELECT policy_id
FROM claims
ORDER BY adjuster_id DESC
