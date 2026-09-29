WITH covered AS (
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
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM covered
    WHERE claimed_amount < (SELECT MAX(claimed_amount) FROM covered)
)
SELECT
    id,
    policy_id,
    status,
    approved_amount,
    product,
    premium,
    CONCAT_WS('-', product, status) AS product_status_label
FROM screened
WHERE product IN ('TRAVEL', 'HEALTH')
ORDER BY id NULLS LAST
