WITH scoped_claims AS (
    -- Scope the claims rows
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
        COALESCE(approved_amount, 0.0) AS approved_amount_filled
    FROM claims
)
SELECT status AS label, reported_at AS event_time, claim_type AS label_2
FROM scoped_claims
WHERE status = 'OPEN'
UNION
SELECT claim_type, reported_at, status
FROM claims
