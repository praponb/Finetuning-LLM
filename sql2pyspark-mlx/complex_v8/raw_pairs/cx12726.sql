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
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        claim_date,
        status,
        claim_type,
        product,
        premium,
        ROUND(claimed_amount * 0.25, 2) AS claimed_amount_adj,
        LENGTH(TRIM(product)) AS product_len
    FROM matched
    WHERE product IS NOT NULL
),
combined_details AS (
    -- Enrich with dimension data
    SELECT s.*, p.start_date AS policy_start_date
    FROM derived s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.start_date >= DATE '2025-01-05'
),
product_stats AS (
    -- Median product_len per product
    SELECT product, PERCENTILE_APPROX(product_len, 0.5) AS median_product_len
    FROM combined_details
    GROUP BY product
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_product_len,
        ROUND(s.product_len - g.median_product_len, 2) AS product_len_minus_median
    FROM combined_details s
    JOIN product_stats g ON g.product = s.product
    WHERE s.product_len >= g.median_product_len
)
SELECT
    id,
    policy_id,
    claim_date,
    status,
    product,
    premium,
    claimed_amount_adj,
    product_len,
    median_product_len,
    product_len_minus_median,
    ROUND(product_len / NULLIF(median_product_len, 0), 3) AS product_len_per_median_product_len,
    CASE WHEN status IN ('REJECTED', 'IN_REVIEW') THEN 'group_a' WHEN status = 'APPROVED' THEN 'group_b' ELSE 'other' END AS status_group
FROM outliers
WHERE claimed_amount_adj > 30060.0
ORDER BY status_group
