WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        ABS(claimed_amount - 28755.2) AS claimed_amount_distance
    FROM joined_ranges
)
-- Final projection
SELECT
    id,
    claim_date,
    claimed_amount,
    approved_amount,
    product,
    premium,
    product_slug,
    claimed_amount_distance,
    INITCAP(TRIM(product_slug)) AS product_slug_clean
FROM labeled
WHERE claimed_amount_distance > 20000.0
ORDER BY product DESC, id
LIMIT 10
