WITH scoped_claims AS (
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
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        claimed_amount + approved_amount AS claimed_amount_plus_approved_amount,
        CONCAT(status, '#', CAST(adjuster_id AS STRING)) AS status_ref
    FROM claims
    WHERE adjuster_id <= 2
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.reported_at,
        s.claimed_amount_bucket,
        s.claimed_amount_plus_approved_amount,
        s.status_ref,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.claim_date AS claim_claim_date,
        c.claim_type AS claim_claim_type
    FROM scoped_claims s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
),
with_children_2 AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.reported_at,
        s.claimed_amount_bucket,
        s.status_ref,
        s.claim_id,
        s.claim_policy_id,
        s.claim_claim_date,
        s.claim_claim_type,
        c.id AS claim_id_2,
        c.status AS claim_status,
        c.approved_amount AS claim_approved_amount
    FROM with_children s
    LEFT JOIN claims c ON c.policy_id = s.claim_policy_id AND c.approved_amount < 15477.7
)
SELECT
    id,
    policy_id,
    adjuster_id,
    approved_amount,
    reported_at,
    claimed_amount_bucket,
    status_ref,
    claim_policy_id,
    claim_claim_date,
    claim_claim_type,
    claim_id_2,
    claim_approved_amount,
    CASE WHEN claim_claim_type IN ('medical', 'collision') THEN 'group_a' WHEN claim_claim_type = 'fire' THEN 'group_b' ELSE 'other' END AS claim_claim_type_group
FROM with_children_2
ORDER BY reported_at NULLS LAST, approved_amount
