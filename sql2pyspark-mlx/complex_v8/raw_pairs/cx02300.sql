WITH covered AS (
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
        LENGTH(TRIM(product)) AS product_len,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
    FROM covered
    WHERE policy_id <= 22
)
SELECT
    id,
    claimed_amount,
    approved_amount,
    product,
    premium,
    product_len,
    approved_amount_bucket,
    DATE_ADD(claim_date, 30) AS claim_date_plus_30d
FROM labeled
WHERE product_len > 6
ORDER BY id DESC
