WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.status,
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
        LEAD(claim_date, 3) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount ASC, id) AS next3_claim_date,
        COUNT(*) OVER (PARTITION BY product, adjuster_id ORDER BY claim_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY product, adjuster_id), 2) AS product_adjuster_id_avg_claimed_amount,
        MAX(claimed_amount) OVER (PARTITION BY product, adjuster_id) AS product_adjuster_id_max_claimed_amount,
        FIRST_VALUE(premium) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium
    FROM in_window
),
combined_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.adjuster_id,
        s.product,
        s.premium,
        s.next3_claim_date,
        s.running_count,
        s.product_adjuster_id_avg_claimed_amount,
        s.product_adjuster_id_max_claimed_amount,
        s.first_premium,
        a.adjuster_name
    FROM metrics s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    adjuster_id,
    product,
    premium,
    running_count,
    product_adjuster_id_avg_claimed_amount,
    product_adjuster_id_max_claimed_amount
FROM combined_details_adjuster
ORDER BY policy_id DESC
