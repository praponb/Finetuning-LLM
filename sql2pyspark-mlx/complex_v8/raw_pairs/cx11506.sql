WITH valid_claims AS (
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
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
    FROM claims
    WHERE adjuster_id >= 5
      AND status IN ('IN_REVIEW', 'PAID')
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.claim_type_slug,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_type AS claim_claim_type
    FROM valid_claims s
    JOIN claims c ON c.policy_id = s.policy_id
),
deduped AS (
    -- Keep the latest row per policy_id, adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        claim_type_slug,
        claim_id,
        claim_approved_amount,
        claim_claimed_amount,
        claim_adjuster_id,
        claim_claim_type
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id, adjuster_id ORDER BY claimed_amount DESC, id DESC, claim_id DESC) AS row_num
        FROM exploded_detail_claims
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with ROLLUP
SELECT
    claim_adjuster_id,
    claim_id,
    COUNT(DISTINCT claim_claim_type) AS distinct_claim_claim_types
FROM deduped
GROUP BY ROLLUP(claim_adjuster_id, claim_id)
ORDER BY claim_adjuster_id NULLS LAST, claim_id NULLS LAST
