WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.claim_type,
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
        *,
        LENGTH(TRIM(product)) AS product_len,
        ABS(premium - 2945.9) AS premium_distance,
        MONTH(claim_date) AS claim_date_month
    FROM covered
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    adjuster_id,
    claim_type,
    product,
    premium,
    product_len,
    premium_distance,
    claim_date_month,
    CASE WHEN claimed_amount >= 29985.9 THEN 'large' WHEN claimed_amount >= 7204.1 THEN 'medium' ELSE 'small' END AS claimed_amount_tier,
    CONCAT(product, '#', CAST(adjuster_id AS STRING)) AS product_ref
FROM labeled
ORDER BY premium, claim_type DESC
