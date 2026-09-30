WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
per_group AS (
    -- Summarise per group
    SELECT product, MAX(premium) AS max_premium, COUNT(*) AS row_count
    FROM matched
    GROUP BY 1
    HAVING MAX(claimed_amount) > 10673.3
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_group
    WHERE row_count > (SELECT AVG(row_count) FROM per_group)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered
    WHERE row_count > (SELECT AVG(row_count) FROM filtered)
)
-- Final projection
SELECT
    product,
    max_premium,
    row_count,
    SUBSTRING(TRIM(product), 1, 2) AS product_prefix
FROM without_matches
ORDER BY row_count DESC, product_prefix NULLS LAST
