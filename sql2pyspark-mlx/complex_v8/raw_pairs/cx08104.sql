WITH in_window AS (
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
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        approved_amount - LAG(approved_amount) OVER (ORDER BY claim_date NULLS LAST, id) AS approved_amount_change,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY product)) AS distinct_policy_id_in_product,
        DENSE_RANK() OVER (ORDER BY claim_date NULLS LAST) AS claim_date_dense_rank,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY product), 2) AS product_avg_claimed_amount,
        NTILE(4) OVER w_all_by_claim_date AS claim_date_quartile
    FROM in_window
    WINDOW w_all_by_claim_date AS (ORDER BY claim_date NULLS LAST, id)
),
adjuster_id_stats AS (
    -- Avg product_avg_claimed_amount per adjuster_id
    SELECT adjuster_id, AVG(product_avg_claimed_amount) AS avg_product_avg_claimed_amount
    FROM metrics
    GROUP BY adjuster_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_product_avg_claimed_amount,
        ROUND(s.product_avg_claimed_amount - g.avg_product_avg_claimed_amount, 2) AS product_avg_claimed_amount_minus_avg
    FROM metrics s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.product_avg_claimed_amount > g.avg_product_avg_claimed_amount
)
SELECT
    id,
    claim_date,
    claimed_amount,
    product,
    premium,
    approved_amount_change,
    distinct_policy_id_in_product,
    claim_date_dense_rank,
    product_avg_claimed_amount,
    product_avg_claimed_amount_minus_avg,
    approved_amount_change - approved_amount AS approved_amount_change_minus_approved_amount,
    DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date
FROM compared
ORDER BY product_avg_claimed_amount
