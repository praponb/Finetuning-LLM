WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_90d,
        SUM(b.approved_amount) AS prior_approved_amount_90d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 90)
    GROUP BY a.id, a.policy_id, a.claim_date
),
joined_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        p.end_date AS policy_end_date,
        p.status AS policy_status
    FROM matched s
    JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    policy_end_date,
    policy_status,
    DATEDIFF(claim_date, policy_end_date) AS days_policy_end_date_to_claim_date,
    DATEDIFF(policy_end_date, claim_date) AS days_claim_date_to_policy_end_date
FROM joined_policy
ORDER BY days_policy_end_date_to_claim_date, claim_date
