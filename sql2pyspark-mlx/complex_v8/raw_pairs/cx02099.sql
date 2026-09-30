WITH prepared AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on
    FROM policyholders
    WHERE risk_score BETWEEN 0.2 AND 0.6
)
SELECT id
FROM prepared
MINUS
SELECT policy_id
FROM claims
WHERE claim_type <> 'delay'
  AND policy_id IS NOT NULL
