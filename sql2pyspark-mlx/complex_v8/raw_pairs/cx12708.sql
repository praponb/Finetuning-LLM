WITH filtered AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        reported_at + INTERVAL 1 DAYS AS reported_at_plus_1d
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND claim_type = 'flood'
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.reported_at AS claim_reported_at,
        c.claim_date AS claim_claim_date
    FROM filtered s
    JOIN claims c ON c.policy_id = s.policy_id
)
-- Subtotals with ROLLUP
SELECT
    adjuster_id,
    claim_id,
    GROUPING_ID() AS grouping_level,
    DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days,
    MAX(claim_claim_date) AS last_claim_claim_date
FROM exploded_detail_claims
GROUP BY ROLLUP(adjuster_id, claim_id)
