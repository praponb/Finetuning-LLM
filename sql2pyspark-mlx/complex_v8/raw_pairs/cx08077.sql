WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.adjuster_id,
        s.product,
        s.premium,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claim_type
    FROM in_window s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY adjuster_id, claim_id)) AS distinct_product_in_adjuster_id_claim_id,
        DENSE_RANK() OVER w_all_by_claim_approved_amount AS claim_approved_amount_dense_rank,
        ROUND(claim_approved_amount / NULLIF(SUM(claim_approved_amount) OVER (PARTITION BY adjuster_id, claim_id), 0), 4) AS claim_approved_amount_share_of_adjuster_id_claim,
        RANK() OVER (ORDER BY claim_approved_amount DESC) AS claim_approved_amount_rank,
        DENSE_RANK() OVER w_all_by_claim_approved_amount_2 AS claim_approved_amount_dense_rank_2
    FROM detail_rows
    WINDOW w_all_by_claim_approved_amount AS (ORDER BY claim_approved_amount DESC, id, claim_id), w_all_by_claim_approved_amount_2 AS (ORDER BY claim_approved_amount DESC)
)
SELECT
    id,
    policy_id,
    claim_date,
    premium,
    claim_approved_amount,
    claim_type,
    claim_approved_amount_share_of_adjuster_id_claim,
    claim_approved_amount_rank,
    claim_approved_amount_dense_rank_2,
    CASE WHEN product IN ('HOME', 'TRAVEL') THEN 'group_a' WHEN product = 'AUTO' THEN 'group_b' ELSE 'other' END AS product_group
FROM metrics
ORDER BY claim_approved_amount_dense_rank_2
