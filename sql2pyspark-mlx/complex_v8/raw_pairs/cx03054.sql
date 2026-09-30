WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        INITCAP(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE claim_type = 'delay'
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claimed_amount AS claim_claimed_amount,
        c.policy_id AS claim_policy_id
    FROM valid s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_id,
    claim_reported_at,
    claim_claimed_amount,
    claim_policy_id,
    DATE_TRUNC('HOUR', claim_reported_at) AS claim_reported_at_hour
FROM detail_rows_claims
ORDER BY claim_id DESC, policy_id DESC NULLS FIRST
