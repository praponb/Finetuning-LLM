WITH covered AS (
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
        claimed_amount,
        claim_type,
        approved_amount,
        product,
        premium,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium,
        ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY claimed_amount, id) AS seq_no,
        COUNT(*) OVER (PARTITION BY product, DATE_TRUNC('MONTH', claim_date)) AS rows_in_product_claim_date_month,
        LEAD(claim_date) OVER (PARTITION BY policy_id ORDER BY claimed_amount, id) AS next_claim_date,
        FIRST_VALUE(approved_amount) OVER (PARTITION BY policy_id ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount,
        MAX(premium) OVER (PARTITION BY product, DATE_TRUNC('MONTH', claim_date)) AS product_claim_date_month_max_premium
    FROM covered
)
SELECT
    COALESCE(product, 'ALL') AS product,
    seq_no,
    COUNT(DISTINCT claim_type) AS distinct_claim_types,
    SUM(CASE WHEN product IS NOT NULL THEN seq_no ELSE 0 END) AS seq_no_product_is_not_null
FROM ranked
GROUP BY GROUPING SETS ((product, seq_no), (product), (seq_no), ())
