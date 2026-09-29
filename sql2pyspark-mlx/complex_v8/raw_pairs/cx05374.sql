WITH scoped AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND policy_id IS NOT NULL
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS(' / ', status, claim_type) AS status_claim_type_label,
        CASE WHEN approved_amount >= 32510.7 THEN 'large' WHEN approved_amount >= 4234.9 THEN 'medium' ELSE 'small' END AS approved_amount_tier
    FROM scoped
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount,
        c.claim_type AS claim_claim_type
    FROM scored s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
deduped AS (
    -- Keep the latest row per claim_claim_type
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        claim_id,
        claim_policy_id,
        claim_reported_at,
        claim_approved_amount,
        claim_claim_type
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_claim_type ORDER BY approved_amount DESC, id DESC, claim_id DESC) AS row_num
        FROM detail_rows_claims
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    policy_id,
    status,
    claim_id,
    claim_approved_amount,
    ABS(approved_amount - 12305.3) AS approved_amount_distance
FROM deduped
ORDER BY approved_amount_distance NULLS LAST
