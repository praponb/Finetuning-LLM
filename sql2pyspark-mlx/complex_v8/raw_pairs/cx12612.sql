WITH valid AS (
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
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month
    FROM claims
    WHERE status = 'REJECTED'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.reported_at,
        p.holder_id AS policy_holder_id,
        p.status AS policy_status
    FROM valid s
    JOIN policies p ON p.id = s.policy_id
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.policy_holder_id,
        s.policy_status,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claim_type AS claim_claim_type
    FROM joined s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.approved_amount < 14993.0
)
SELECT *
FROM (
    SELECT claim_claim_type, claim_type, claim_approved_amount
    FROM with_children
) src
PIVOT (
    SUM(claim_approved_amount)
    FOR claim_type IN ('flood', 'delay', 'collision')
)
ORDER BY claim_claim_type
