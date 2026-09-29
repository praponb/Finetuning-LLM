WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_details_policy AS (
    -- Join policies
    SELECT
        s.*,
        p.end_date AS policy_end_date,
        p.status AS policy_status,
        po.region AS holder_region,
        po.segment AS holder_segment
    FROM joined_ranges s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
    WHERE p.end_date BETWEEN DATE '2025-12-05' AND DATE '2026-11-25'
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_details_policy s
    WHERE s.policy_id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE MONTH(c.reported_at) IN (1, 2)
        )
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claim_type,
        s.adjuster_id,
        s.product,
        s.premium,
        s.policy_end_date,
        s.policy_status,
        s.holder_region,
        s.holder_segment,
        a.team AS adjuster_team,
        a.adjuster_name,
        a.level AS adjuster_level,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date
    FROM screened s
    JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    adjuster_id,
    product,
    premium,
    policy_end_date,
    holder_region,
    holder_segment,
    adjuster_name,
    policy_premium,
    policy_start_date
FROM enriched
WHERE claim_type = 'fire'
ORDER BY policy_end_date, id
LIMIT 20
