WITH prepared_claims AS (
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
    WHERE status IS NOT NULL
      AND claim_type = 'delay'
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claim_date AS claim_claim_date,
        c.approved_amount AS claim_approved_amount
    FROM prepared_claims s
    LEFT JOIN claims c ON c.policy_id = s.policy_id
)
-- Subtotals with GROUPING SETS
SELECT
    adjuster_id,
    policy_id,
    claim_id,
    COUNT(*) AS row_count
FROM detail_rows_claims
GROUP BY GROUPING SETS ((adjuster_id, policy_id, claim_id), (adjuster_id), ())
ORDER BY adjuster_id NULLS LAST, policy_id NULLS LAST, claim_id NULLS LAST
