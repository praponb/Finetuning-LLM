WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY approved_amount DESC), 3) AS approved_amount_cume_dist,
        MIN(premium) OVER (PARTITION BY claim_type) AS claim_type_min_premium,
        ROUND(CUME_DIST() OVER (PARTITION BY product, claim_type ORDER BY premium, id), 3) AS premium_cume_dist,
        RANK() OVER (PARTITION BY product, policy_id ORDER BY premium ASC, id) AS premium_rank,
        ROUND(CUME_DIST() OVER (ORDER BY approved_amount DESC, id), 3) AS approved_amount_cume_dist_2
    FROM joined_ranges
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    approved_amount,
    claim_type,
    premium,
    approved_amount_cume_dist,
    claim_type_min_premium,
    premium_cume_dist,
    premium_rank,
    approved_amount_cume_dist_2,
    approved_amount + premium AS approved_amount_plus_premium
FROM metrics
