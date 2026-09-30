WITH in_window AS (
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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
enriched AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.claimed_amount,
        s.claim_type,
        s.product,
        s.premium,
        p.premium AS policy_premium,
        p.product AS policy_product,
        p.holder_id AS policy_holder_id
    FROM in_window s
    JOIN policies p ON p.id = s.policy_id AND p.start_date < DATE '2025-03-21'
    WHERE p.holder_id >= 10
)
-- Final projection
SELECT
    id,
    policy_id,
    claimed_amount,
    claim_type,
    product,
    premium,
    policy_premium,
    policy_product,
    CASE WHEN product IN ('HOME', 'HEALTH') THEN 'group_a' WHEN product = 'TRAVEL' THEN 'group_b' ELSE 'other' END AS product_group
FROM enriched
ORDER BY product DESC, claim_type NULLS LAST, id
LIMIT 10
