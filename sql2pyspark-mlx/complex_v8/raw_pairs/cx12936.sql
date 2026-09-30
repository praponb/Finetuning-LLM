WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at,
        LENGTH(TRIM(status)) AS status_len,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        ABS(approved_amount - 10355.0) AS approved_amount_distance
    FROM claims
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.status_len,
        s.approved_amount_filled,
        c.id AS claim_id,
        c.claim_date AS claim_claim_date,
        c.policy_id AS claim_policy_id,
        c.reported_at AS claim_reported_at,
        c.status AS claim_status
    FROM filtered_claims s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
)
-- Final projection
SELECT
    id,
    adjuster_id,
    status_len,
    claim_id,
    claim_claim_date,
    claim_policy_id,
    claim_reported_at
FROM exploded_detail_claims
ORDER BY id, claim_claim_date DESC NULLS FIRST
