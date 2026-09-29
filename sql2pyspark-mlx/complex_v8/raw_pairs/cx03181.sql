WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.status,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(premium) OVER (PARTITION BY product, policy_id ORDER BY claim_date ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS premium_sum_2_prior,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY product, policy_id ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount
    FROM matched
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND HOUR(c.reported_at) BETWEEN 6 AND 10
        )
)
SELECT
    status,
    policy_id,
    GROUPING(status) AS is_total_status,
    ROUND(SUM(premium) / NULLIF(SUM(premium_sum_2_prior), 0), 3) AS premium_to_premium_sum_2_prior_ratio,
    ROUND(SUM(premium_sum_2_prior) / NULLIF(SUM(claimed_amount), 0), 3) AS premium_sum_2_prior_to_claimed_amount_ratio
FROM qualified
GROUP BY CUBE(status, policy_id)
ORDER BY status NULLS LAST, policy_id NULLS LAST
