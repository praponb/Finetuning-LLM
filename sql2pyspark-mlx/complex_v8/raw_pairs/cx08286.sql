WITH joined_ranges AS (
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
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        claim_date,
        claimed_amount,
        product,
        premium,
        SIZE(COLLECT_SET(policy_id) OVER w_product_claim_type) AS distinct_policy_id_in_product_claim_type,
        SUM(premium) OVER (PARTITION BY product, claim_type) AS product_claim_type_sum_premium,
        ROUND(AVG(approved_amount) OVER w_product_claim_type, 2) AS product_claim_type_avg_approved_amount
    FROM joined_ranges
    WINDOW w_product_claim_type AS (PARTITION BY product, claim_type)
),
product_stats AS (
    -- Avg product_claim_type_avg_approved_amount per product
    SELECT product, AVG(product_claim_type_avg_approved_amount) AS avg_product_claim_type_avg_approved_amount
    FROM ranked
    GROUP BY product
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_product_claim_type_avg_approved_amount,
        ROUND(s.product_claim_type_avg_approved_amount - g.avg_product_claim_type_avg_approved_amount, 2) AS product_claim_type_avg_approved_amount_minus_avg
    FROM ranked s
    JOIN product_stats g ON g.product = s.product
    WHERE s.product_claim_type_avg_approved_amount > g.avg_product_claim_type_avg_approved_amount
)
SELECT
    id,
    product,
    distinct_policy_id_in_product_claim_type,
    product_claim_type_sum_premium,
    product_claim_type_avg_approved_amount,
    avg_product_claim_type_avg_approved_amount,
    product_claim_type_avg_approved_amount_minus_avg,
    DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_label,
    COALESCE(premium, 0.0) AS premium_filled
FROM compared
WHERE product_claim_type_sum_premium <= 27590.0
ORDER BY distinct_policy_id_in_product_claim_type
