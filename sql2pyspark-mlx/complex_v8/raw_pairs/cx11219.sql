WITH recent_claims AS (
    -- Filter claims
    SELECT *
    FROM claims
    WHERE claim_type IS NOT NULL
),
enriched AS (
    -- Join policies
    SELECT
        s.*,
        p.premium AS policy_premium,
        p.product AS policy_product,
        p.holder_id AS policy_holder_id,
        po.segment AS holder_segment
    FROM recent_claims s
    JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
),
latest AS (
    -- Keep the latest row per holder_segment, status
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
        policy_premium,
        policy_product,
        policy_holder_id,
        holder_segment
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_segment, status ORDER BY claimed_amount DESC, id DESC) AS recency_rank
        FROM enriched
    ) ranked
    WHERE recency_rank = 1
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.policy_product,
        s.policy_holder_id,
        s.holder_segment,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claim_date AS claim_claim_date,
        c.claim_type AS claim_claim_type,
        c.approved_amount AS claim_approved_amount
    FROM latest s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
)
SELECT id, claim_id, measure, reading
FROM (
    SELECT id, claim_id, claim_approved_amount, claimed_amount
    FROM exploded_detail_claims
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (claim_approved_amount, claimed_amount)
) unp
