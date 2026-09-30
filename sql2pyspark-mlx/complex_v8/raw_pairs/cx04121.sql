WITH scoped AS (
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
        reported_at
    FROM claims
    WHERE claim_type IS NOT NULL
)
SELECT 'claim' AS origin, id AS qty, claim_type
FROM scoped
UNION ALL
SELECT 'claim_2' AS origin, policy_id, claim_type
FROM claims
