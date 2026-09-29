WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        status,
        product,
        premium,
        CASE WHEN product IN ('TRAVEL', 'HEALTH') THEN 'group_a' WHEN product = 'HOME' THEN 'group_b' ELSE 'other' END AS product_group,
        COALESCE(premium, 0.0) AS premium_filled
    FROM joined_ranges
    WHERE claim_date >= DATE '2024-06-13'
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.product_group,
        s.premium_filled,
        c.id AS claim_id,
        c.status AS claim_status,
        c.approved_amount AS claim_approved_amount,
        c.claim_type
    FROM enriched s
    LEFT JOIN claims c ON c.policy_id = s.policy_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_date,
        product_group,
        premium_filled,
        claim_id,
        claim_approved_amount,
        claim_type,
        ROUND(premium_filled - AVG(premium_filled) OVER (PARTITION BY claim_status), 2) AS premium_filled_vs_claim_status_avg,
        ROUND(CUME_DIST() OVER w_all_by_claimed_amount, 3) AS claimed_amount_cume_dist,
        ROUND(claim_approved_amount - AVG(claim_approved_amount) OVER (PARTITION BY claim_status), 2) AS claim_approved_amount_vs_claim_status_avg,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY claim_status), 0), 4) AS claimed_amount_share_of_claim_status,
        ROUND(premium_filled / NULLIF(SUM(premium_filled) OVER (PARTITION BY claim_status), 0), 4) AS premium_filled_share_of_claim_status,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_claim_status, 2) AS claimed_amount_vs_claim_status_avg
    FROM with_children
    WINDOW w_all_by_claimed_amount AS (ORDER BY claimed_amount DESC, id, claim_id), w_claim_status AS (PARTITION BY claim_status)
)
-- Subtotals with GROUPING SETS
SELECT
    claim_id,
    policy_id,
    product_group,
    COUNT(*) AS n_rows
FROM with_windows
GROUP BY GROUPING SETS ((claim_id, policy_id, product_group), (claim_id), ())
ORDER BY claim_id NULLS LAST, policy_id NULLS LAST, product_group NULLS LAST
