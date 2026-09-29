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
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        claim_date,
        claimed_amount,
        product,
        ROUND(PERCENT_RANK() OVER (ORDER BY claimed_amount NULLS LAST), 3) AS claimed_amount_percent_rank,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY YEAR(claim_date)), 2) AS claim_date_year_avg_claimed_amount
    FROM in_window
),
product_stats AS (
    -- Avg claimed_amount per product
    SELECT product, AVG(claimed_amount) AS avg_claimed_amount
    FROM ranked
    GROUP BY product
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claimed_amount,
        ROUND(s.claimed_amount - g.avg_claimed_amount, 2) AS claimed_amount_minus_avg
    FROM ranked s
    JOIN product_stats g ON g.product = s.product
    WHERE s.claimed_amount > g.avg_claimed_amount
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        claim_date,
        claimed_amount,
        product,
        claimed_amount_percent_rank,
        claimed_amount_minus_avg,
        SUBSTRING(TRIM(product), 1, 4) AS product_prefix,
        ADD_MONTHS(claim_date, 3) AS claim_date_plus_3m,
        CASE WHEN claimed_amount_minus_avg >= 10000.0 THEN 'A' WHEN claimed_amount_minus_avg >= -25000.0 THEN 'B' ELSE 'C' END AS claimed_amount_minus_avg_tier
    FROM outliers
    WHERE claimed_amount_minus_avg < 10000.0
)
SELECT
    id,
    claim_date,
    claimed_amount,
    product,
    claimed_amount_percent_rank,
    claimed_amount_minus_avg,
    product_prefix,
    claim_date_plus_3m,
    claimed_amount_minus_avg_tier
FROM derived
ORDER BY claimed_amount
