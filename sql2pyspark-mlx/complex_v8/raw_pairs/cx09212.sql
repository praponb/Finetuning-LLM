WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.status,
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
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.product,
        s.premium,
        p.end_date AS policy_end_date,
        p.premium AS policy_premium
    FROM matched s
    LEFT JOIN policies p ON p.id = s.policy_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, premium, approved_amount, claimed_amount
    FROM combined_details
) src
UNPIVOT (
    value FOR metric_name IN (premium AS m_premium, approved_amount AS approved_amount_v, claimed_amount AS claimed_amount_v)
) unp
ORDER BY id, metric_name
