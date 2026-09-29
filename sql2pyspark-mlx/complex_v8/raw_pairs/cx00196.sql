WITH covered AS (
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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        claim_date,
        approved_amount,
        product,
        premium,
        NTILE(4) OVER (PARTITION BY policy_id, product ORDER BY claim_date NULLS LAST, id) AS claim_date_quartile,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY policy_id), 0), 4) AS premium_share_of_policy_id,
        COUNT(*) OVER w_policy_id AS rows_in_policy_id,
        ROUND(CUME_DIST() OVER w_policy_id_by_claim_date_ties, 3) AS claim_date_cume_dist,
        ROUND(AVG(premium) OVER (PARTITION BY policy_id ORDER BY claim_date ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS premium_avg_2
    FROM covered
    WINDOW w_policy_id AS (PARTITION BY policy_id), w_policy_id_by_claim_date_ties AS (PARTITION BY policy_id ORDER BY claim_date ASC NULLS LAST)
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, MAX(approved_amount) AS max_claim_approved_amount
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.max_claim_approved_amount
    FROM ranked s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    product,
    premium,
    claim_date_quartile,
    rows_in_policy_id,
    premium_avg_2,
    max_claim_approved_amount
FROM summarized_rows
ORDER BY product NULLS LAST
