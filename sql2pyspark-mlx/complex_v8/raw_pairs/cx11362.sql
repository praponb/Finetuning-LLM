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
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM covered s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.status IN ('OPEN', 'PAID', 'REJECTED')
        )
      AND s.policy_id IS NOT NULL
),
joined AS (
    -- Join adjusters
    SELECT s.*, a.team AS adjuster_team, a.adjuster_name
    FROM qualified s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
policy_id_stats AS (
    -- Avg premium per policy_id
    SELECT policy_id, AVG(premium) AS avg_premium
    FROM joined
    GROUP BY policy_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM joined s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.premium > g.avg_premium
)
SELECT *
FROM (
    SELECT adjuster_id, claim_type, premium
    FROM compared
) src
PIVOT (
    AVG(premium)
    FOR claim_type IN ('medical', 'collision', 'flood', 'delay')
)
