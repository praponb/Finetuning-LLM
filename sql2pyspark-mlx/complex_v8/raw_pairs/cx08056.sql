SELECT id, status, premium
FROM policies
UNION
SELECT policy_id AS id, claim_type AS status, approved_amount AS premium
FROM claims
WHERE policy_id IS NOT NULL
