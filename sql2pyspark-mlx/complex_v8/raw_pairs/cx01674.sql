WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date
    FROM policies
    WHERE holder_id >= 2
),
enriched_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on
    FROM scoped_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.risk_score <= 0.6
),
without_matches AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM enriched_holder s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.premium > 2289.0
              AND p.start_date < DATE '2025-05-02'
        )
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        c.id AS claim_id,
        c.claim_date,
        c.claimed_amount AS claim_claimed_amount,
        c.adjuster_id AS claim_adjuster_id,
        c.reported_at AS claim_reported_at
    FROM without_matches s
    JOIN claims c ON c.policy_id = s.id AND c.adjuster_id IS NOT NULL
)
SELECT
    holder_id,
    claim_id,
    GROUPING_ID() AS grouping_level,
    MAX(claim_reported_at) AS last_claim_reported_at,
    PERCENTILE_APPROX(claim_claimed_amount, 0.5) AS median_claim_claimed_amount,
    MAX(claim_date) AS last_claim_date
FROM line_level_claims
GROUP BY GROUPING SETS ((holder_id, claim_id), (holder_id))
