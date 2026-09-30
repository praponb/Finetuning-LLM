WITH in_window AS (
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
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_claim_date_year_by_approved AS approved_amount_rank,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY claim_type, policy_id)) AS distinct_product_in_claim_type_policy_id
    FROM in_window
    WINDOW w_claim_date_year_by_approved AS (PARTITION BY YEAR(claim_date) ORDER BY approved_amount ASC, id)
)
-- Final projection
SELECT
    id,
    claim_date,
    claimed_amount,
    approved_amount,
    premium,
    approved_amount_rank,
    distinct_product_in_claim_type_policy_id,
    ABS(approved_amount_rank - 5) AS approved_amount_rank_distance
FROM windowed
ORDER BY approved_amount_rank_distance DESC, premium NULLS LAST
