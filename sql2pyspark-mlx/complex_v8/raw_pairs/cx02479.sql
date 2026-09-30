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
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.level AS adjuster_level,
        a.adjuster_name,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date
    FROM covered s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.team IS NOT NULL
    JOIN policies p ON p.id = s.policy_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(claimed_amount, 2) OVER (ORDER BY policy_start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        LEAD(claim_date, 2) OVER (ORDER BY policy_start_date ASC, id) AS next2_claim_date
    FROM with_details
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
      AND s.next2_claim_date IS NOT NULL
)
SELECT
    adjuster_id,
    policy_id,
    claim_type,
    GROUPING(adjuster_id) AS is_total_adjuster_id,
    ROUND(AVG(policy_premium), 2) AS avg_policy_premium,
    SUM(claimed_amount) AS total_claimed_amount,
    COUNT(DISTINCT claim_type) AS distinct_claim_types
FROM screened
GROUP BY GROUPING SETS ((adjuster_id, policy_id, claim_type), (adjuster_id), ())
