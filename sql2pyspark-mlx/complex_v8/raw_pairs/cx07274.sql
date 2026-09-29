WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status
    FROM claims
    WHERE status = 'IN_REVIEW'
      AND claim_type = 'collision'
)
SELECT id, reported_at AS event_time, claim_type AS category
FROM claims
WHERE status = 'APPROVED'
UNION
SELECT policy_id AS id, reported_at AS event_time, claim_type AS category
FROM claims
WHERE claim_type IN ('fire', 'collision', 'flood')
