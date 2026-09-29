WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        approved_amount - claimed_amount AS approved_amount_minus_claimed_amount
    FROM matched
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    status,
    approved_amount,
    product,
    premium,
    approved_amount_minus_claimed_amount,
    CASE WHEN claimed_amount >= 33690.0 THEN 'A' WHEN claimed_amount >= 13635.1 THEN 'B' ELSE 'C' END AS claimed_amount_tier,
    DATEDIFF(DATE '2025-12-31', claim_date) AS days_since_claim_date
FROM annotated
