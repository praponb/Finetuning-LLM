WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        claim_date,
        status,
        adjuster_id,
        product,
        premium,
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref
    FROM in_window
    WHERE status IS NOT NULL
)
-- Final projection
SELECT
    id,
    status,
    adjuster_id,
    status_ref,
    LOWER(REGEXP_REPLACE(TRIM(status_ref), ' ', '_')) AS status_ref_slug,
    CONCAT(product, '#', CAST(adjuster_id AS STRING)) AS product_ref
FROM labeled
ORDER BY status, status_ref, id
LIMIT 10
