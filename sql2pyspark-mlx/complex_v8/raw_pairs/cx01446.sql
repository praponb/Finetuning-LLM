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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_parent AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.adjuster_id,
        s.claim_type,
        s.product,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM covered s
    JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT
    id,
    policy_id,
    claimed_amount,
    claim_type,
    product,
    adjuster_team,
    adjuster_name,
    COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
FROM with_parent
WHERE adjuster_id <= 4
ORDER BY adjuster_name, id
LIMIT 20
