WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM matched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claim_date IS NOT NULL
              AND HOUR(c.reported_at) BETWEEN 10 AND 16
        )
      AND s.premium > 1132.1
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(premium) OVER (PARTITION BY product ORDER BY claim_date DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_max_2,
        RANK() OVER (PARTITION BY product ORDER BY claim_date DESC, id) AS claim_date_rank,
        ROUND(AVG(claimed_amount) OVER w_claim_date_year_claim_type, 2) AS claim_date_year_claim_type_avg_claimed_amount,
        LEAD(claim_date) OVER (PARTITION BY product ORDER BY claim_date DESC, id) AS next_claim_date
    FROM with_activity
    WINDOW w_product_by_claim_date AS (PARTITION BY product ORDER BY claim_date DESC, id), w_claim_date_year_claim_type AS (PARTITION BY YEAR(claim_date), claim_type)
)
SELECT
    policy_id,
    COALESCE(product, 'ALL') AS product,
    COALESCE(claim_type, 'ALL') AS claim_type,
    GROUPING(policy_id) AS is_total_policy_id,
    SUM(claim_date_rank) AS total_claim_date_rank,
    SUM(claimed_amount) AS total_claimed_amount
FROM windowed
GROUP BY GROUPING SETS ((policy_id, product, claim_type), (policy_id), (product), ())
ORDER BY policy_id NULLS LAST, product NULLS LAST, claim_type NULLS LAST
