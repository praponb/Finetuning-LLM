WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        claim_type,
        product,
        premium,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product), 0), 4) AS premium_share_of_product,
        LEAD(claimed_amount) OVER (PARTITION BY claim_type ORDER BY claimed_amount DESC, id) AS next_claimed_amount,
        MIN(claimed_amount) OVER (PARTITION BY product) AS product_min_claimed_amount,
        MAX(claimed_amount) OVER (PARTITION BY product) AS product_max_claimed_amount,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_type ORDER BY claimed_amount), 3) AS claimed_amount_cume_dist,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product
    FROM matched
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        product,
        premium,
        premium_share_of_product,
        product_min_claimed_amount,
        product_max_claimed_amount,
        rows_in_product,
        MAX(premium_share_of_product) OVER (PARTITION BY product, claim_type ORDER BY claimed_amount ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_share_of_product_max_2,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY YEAR(claim_date))) AS distinct_claim_type_in_claim_date_year,
        ROUND(AVG(product_min_claimed_amount) OVER (PARTITION BY YEAR(claim_date)), 2) AS claim_date_year_avg_product_min_claimed_amount,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY YEAR(claim_date))) AS distinct_product_in_claim_date_year,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY product, claim_type ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        ROUND(CUME_DIST() OVER (PARTITION BY product, claim_type ORDER BY claimed_amount ASC), 3) AS claimed_amount_cume_dist_2
    FROM ranked
)
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    product,
    premium,
    product_min_claimed_amount,
    product_max_claimed_amount,
    rows_in_product,
    premium_share_of_product_max_2,
    claim_date_year_avg_product_min_claimed_amount,
    distinct_product_in_claim_date_year,
    last_known_claim_date
FROM windowed
ORDER BY distinct_product_in_claim_date_year, premium, id
LIMIT 20
