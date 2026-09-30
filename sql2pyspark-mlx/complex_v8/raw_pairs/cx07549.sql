WITH filtered_policys AS (
    -- Scope the policies rows
    SELECT *
    FROM policies
    WHERE holder_id IS NOT NULL
      AND status IN ('CANCELLED', 'LAPSED', 'ACTIVE')
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.status,
        p.joined_on AS holder_joined_on,
        p.region AS holder_region,
        p.segment AS holder_segment
    FROM filtered_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.holder_joined_on,
        s.holder_region,
        c.id AS claim_id,
        c.claim_date,
        c.claimed_amount AS claim_claimed_amount,
        c.adjuster_id AS claim_adjuster_id,
        c.status AS claim_status
    FROM combined_details s
    JOIN claims c ON c.policy_id = s.id
)
SELECT id, claim_id, field, metric_value
FROM (
    SELECT id, claim_id, claim_claimed_amount, premium
    FROM exploded_detail_claims
) src
UNPIVOT (
    metric_value FOR field IN (claim_claimed_amount, premium)
) unp
