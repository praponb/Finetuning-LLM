WITH covered AS (
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
summary AS (
    -- Summarise per group
    SELECT
        product,
        status,
        claim_type,
        MIN(claimed_amount) AS min_claimed_amount,
        ROUND(AVG(claimed_amount), 2) AS avg_claimed_amount,
        SUM(CASE WHEN claim_type IN ('delay', 'theft', 'flood') THEN 1 ELSE 0 END) AS n_claim_type_in_delay_theft_fl
    FROM covered
    GROUP BY product, status, claim_type
)
-- Final projection
SELECT
    product,
    status,
    min_claimed_amount,
    avg_claimed_amount,
    n_claim_type_in_delay_theft_fl
FROM summary
WHERE avg_claimed_amount < 42515.0
ORDER BY product DESC, status
