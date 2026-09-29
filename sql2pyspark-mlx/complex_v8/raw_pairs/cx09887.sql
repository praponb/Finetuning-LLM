WITH in_window AS (
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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM in_window s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND MONTH(c.reported_at) IN (1, 2)
              AND c.adjuster_id IS NOT NULL
        )
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        status,
        product,
        premium,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        ROUND(PERCENT_RANK() OVER w_all_by_premium_ties, 3) AS premium_percent_rank,
        RANK() OVER (PARTITION BY policy_id ORDER BY claim_date, id) AS claim_date_rank,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_id ORDER BY claim_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_2_prior,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_product_policy_id, 2) AS claimed_amount_vs_product_policy_id_avg
    FROM without_matches
    WINDOW w_all_by_premium_ties AS (ORDER BY premium), w_product_policy_id AS (PARTITION BY product, policy_id)
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    status,
    product,
    premium,
    premium_percent_rank,
    claim_date_rank,
    claimed_amount_avg_2_prior,
    claimed_amount_vs_product_policy_id_avg,
    SUBSTRING(TRIM(product), 1, 3) AS product_prefix,
    CASE WHEN product IN ('TRAVEL', 'LIFE') THEN 'group_a' WHEN product = 'HOME' THEN 'group_b' ELSE 'other' END AS product_group
FROM sequenced
ORDER BY premium, claim_date_rank
