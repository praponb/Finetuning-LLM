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
enriched AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.adjuster_id,
        s.status,
        s.product,
        s.premium,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM in_window s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(premium / NULLIF(SUM(premium) OVER w_claim_date_year_product, 0), 4) AS premium_share_of_claim_date_year_product,
        MIN(adjuster_level) OVER (PARTITION BY YEAR(claim_date), product) AS claim_date_year_product_min_adjuster_level,
        SUM(claimed_amount) OVER (PARTITION BY policy_id, status ORDER BY adjuster_level NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        COUNT(*) OVER (PARTITION BY YEAR(claim_date), product) AS rows_in_claim_date_year_product
    FROM enriched
    WINDOW w_claim_date_year_product AS (PARTITION BY YEAR(claim_date), product)
)
-- Subtotals with GROUPING SETS
SELECT
    status,
    product,
    DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days,
    PERCENTILE_APPROX(premium, 0.5) AS median_premium
FROM with_windows
GROUP BY GROUPING SETS ((status, product), (status), (product), ())
ORDER BY status NULLS LAST, product NULLS LAST
