WITH valid_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CONCAT(claim_type, '#', CAST(policy_id AS STRING)) AS claim_type_ref
    FROM claims
    WHERE claim_date IS NOT NULL
)
SELECT adjuster_id
FROM valid_claims
WHERE approved_amount >= 4554.8
  AND claim_type_ref IS NOT NULL
MINUS
SELECT policy_id AS adjuster_id
FROM claims
WHERE status = 'OPEN'
