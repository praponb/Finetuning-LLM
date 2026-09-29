WITH scoped_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        reported_at,
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM claims
    WHERE policy_id >= 28
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.status_ref,
        c.id AS claim_id,
        c.claim_type AS claim_claim_type,
        c.approved_amount AS claim_approved_amount,
        c.policy_id AS claim_policy_id,
        c.claim_date
    FROM scoped_claims s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
),
scored AS (
    -- Add calculated columns
    SELECT *, DAYOFWEEK(claim_date) AS claim_date_dayofweek
    FROM with_children_claims
)
-- Subtotals with CUBE
SELECT
    claim_id,
    COALESCE(claim_claim_type, 'ALL') AS claim_claim_type,
    COALESCE(claim_type, 'ALL') AS claim_type,
    COUNT(*) AS cnt,
    ROUND(AVG(claim_approved_amount), 2) AS avg_claim_approved_amount,
    ROUND(AVG(claim_date_dayofweek), 2) AS avg_claim_date_dayofweek
FROM scored
GROUP BY CUBE(claim_id, claim_claim_type, claim_type)
