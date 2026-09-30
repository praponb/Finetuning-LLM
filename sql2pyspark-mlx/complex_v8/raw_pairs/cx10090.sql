SELECT id, premium AS amount, holder_id
FROM policies
UNION
SELECT adjuster_id AS id, approved_amount AS amount, policy_id AS holder_id
FROM claims
WHERE claimed_amount <= 44921.3
  AND policy_id IS NOT NULL
ORDER BY id, holder_id
