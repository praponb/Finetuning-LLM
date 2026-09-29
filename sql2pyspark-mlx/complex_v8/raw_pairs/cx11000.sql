WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.product,
        s.premium,
        c.id AS claim_id,
        c.claim_type,
        c.status AS claim_status,
        c.claim_date AS claim_claim_date
    FROM joined_ranges s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    premium,
    claim_id,
    claim_type,
    claim_status,
    claim_claim_date,
    LOWER(TRIM(claim_type)) AS claim_type_clean
FROM exploded_detail_claims
WHERE policy_id <= 3
ORDER BY policy_id DESC, premium DESC
