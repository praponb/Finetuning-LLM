WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        reported_at,
        ABS(claimed_amount - 14019.8) AS claimed_amount_distance,
        QUARTER(reported_at) AS reported_at_quarter
    FROM claims
    WHERE adjuster_id <= 5
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.reported_at,
        s.claimed_amount_distance,
        s.reported_at_quarter,
        p.premium AS policy_premium,
        p.product AS policy_product
    FROM valid s
    LEFT JOIN policies p ON p.id = s.policy_id AND p.end_date >= DATE '2026-01-20'
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, claimed_amount_distance, policy_premium
    FROM with_details
) src
UNPIVOT (
    amount FOR measure IN (claimed_amount_distance AS claimed_amount_distance_v, policy_premium AS m_policy_premium)
) unp
ORDER BY id, measure
