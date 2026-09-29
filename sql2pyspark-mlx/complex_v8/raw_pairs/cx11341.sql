WITH matched AS (
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
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter,
        GREATEST(premium, claimed_amount) AS greatest_premium_claimed_amount
    FROM matched
    WHERE adjuster_id <= 7
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(status, '#', CAST(premium_bucket AS STRING)) AS status_ref,
        LEAST(premium_bucket, premium) AS least_premium_bucket_premium,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref
    FROM enriched
)
SELECT
    COALESCE(status, 'ALL') AS status,
    adjuster_id,
    policy_id,
    COUNT(*) AS n_rows,
    ROUND(STDDEV(premium), 2) AS stddev_premium
FROM scored
GROUP BY ROLLUP(status, adjuster_id, policy_id)
