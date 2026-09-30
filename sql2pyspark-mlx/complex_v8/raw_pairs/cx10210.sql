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
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.product,
        c.id AS claim_id,
        c.claim_type,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at
    FROM covered s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    product,
    claim_id,
    claim_type,
    claim_approved_amount,
    claim_reported_at,
    LENGTH(TRIM(product)) AS product_len,
    SUBSTRING(TRIM(product), 1, 3) AS product_prefix
FROM detail_rows
ORDER BY claim_reported_at DESC NULLS FIRST, id
