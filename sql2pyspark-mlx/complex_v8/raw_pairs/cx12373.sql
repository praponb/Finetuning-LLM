WITH joined_ranges AS (
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
rollup_base AS (
    -- Summarise per group
    SELECT
        product,
        claim_type,
        status,
        COUNT(*) AS n_rows,
        DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days,
        ROUND(100 * COUNT(CASE WHEN product = 'AUTO' THEN 1 END) / COUNT(*), 1) AS pct_product_auto
    FROM joined_ranges
    WHERE status NOT IN ('APPROVED', 'OPEN')
    GROUP BY product, claim_type, status
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    product,
    claim_type,
    status,
    n_rows,
    claim_date_span_days,
    pct_product_auto,
    FLOOR(pct_product_auto / 25) * 25 AS pct_product_auto_bucket,
    LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
FROM rollup_base
WHERE status IS NOT NULL
ORDER BY product
