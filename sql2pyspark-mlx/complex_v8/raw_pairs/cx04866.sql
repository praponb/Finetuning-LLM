WITH covered AS (
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
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        MIN(claimed_amount) AS min_claim_claimed_amount,
        COUNT(*) FILTER (WHERE claim_type IS NOT NULL) AS claim_claim_type_count
    FROM claims
    GROUP BY adjuster_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.min_claim_claimed_amount,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count
    FROM covered s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
)
SELECT
    id,
    claim_date,
    claimed_amount,
    adjuster_id,
    premium,
    min_claim_claimed_amount,
    claim_claim_type_count,
    ROUND(approved_amount * 0.25, 2) AS approved_amount_adj,
    LEAST(min_claim_claimed_amount, premium) AS least_min_claim_claimed_amount_premium
FROM with_stats
ORDER BY approved_amount_adj DESC, premium DESC NULLS FIRST
