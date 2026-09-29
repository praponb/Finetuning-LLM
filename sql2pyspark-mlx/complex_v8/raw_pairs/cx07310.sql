WITH in_window AS (
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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
combined_details AS (
    -- Attach policy attributes
    SELECT
        s.*,
        p.product AS policy_product,
        po.region AS holder_region,
        po.holder_name
    FROM in_window s
    JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
)
-- Final projection
SELECT
    id,
    claim_date,
    claimed_amount,
    approved_amount,
    product,
    premium,
    policy_product,
    holder_region,
    holder_name,
    DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label
FROM combined_details
ORDER BY holder_region NULLS LAST
