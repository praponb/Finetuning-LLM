WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_parent AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claim_type,
        s.adjuster_id,
        p.start_date AS policy_start_date,
        p.product AS policy_product,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM joined_ranges s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT *
FROM (
    SELECT adjuster_id, policy_product, adjuster_level
    FROM with_parent
) src
PIVOT (
    AVG(adjuster_level) AS avg_v, COUNT(adjuster_level) AS n
    FOR policy_product IN ('LIFE', 'HEALTH', 'TRAVEL', 'HOME')
)
