WITH matched AS (
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
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(premium - AVG(premium) OVER w_status_product, 2) AS premium_vs_status_product_avg,
        COUNT(*) OVER (PARTITION BY policy_id ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_id ORDER BY claim_date, id), 3) AS claim_date_percent_rank,
        SUM(claimed_amount) OVER (PARTITION BY adjuster_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        DENSE_RANK() OVER w_adjuster_id_by_claim_date_ti AS claim_date_dense_rank,
        LEAD(premium) OVER (PARTITION BY policy_id ORDER BY claimed_amount NULLS LAST, id) AS next_premium
    FROM matched
    WINDOW w_status_product AS (PARTITION BY status, product), w_adjuster_id_by_claim_date_ti AS (PARTITION BY adjuster_id ORDER BY claim_date)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(premium - AVG(premium) OVER (PARTITION BY MONTH(claim_date)), 2) AS premium_vs_claim_date_month_avg,
        RANK() OVER (PARTITION BY YEAR(claim_date) ORDER BY running_claimed_amount DESC) AS running_claimed_amount_rank,
        MIN(running_count) OVER (PARTITION BY YEAR(claim_date) ORDER BY running_claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_running_count_so_far,
        COUNT(*) OVER w_claim_date_month AS rows_in_claim_date_month
    FROM with_trends
    WINDOW w_claim_date_month AS (PARTITION BY MONTH(claim_date))
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    adjuster_id,
    premium,
    premium_vs_status_product_avg,
    running_count,
    claim_date_percent_rank,
    running_claimed_amount,
    claim_date_dense_rank,
    next_premium,
    premium_vs_claim_date_month_avg,
    running_claimed_amount_rank,
    min_running_count_so_far,
    CASE WHEN premium >= 3521.8 THEN 'large' WHEN premium >= 1132.1 THEN 'medium' ELSE 'small' END AS premium_tier
FROM metrics
ORDER BY running_count DESC
