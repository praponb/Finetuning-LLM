WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_30d,
        SUM(b.claimed_amount) AS prior_claimed_amount_30d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 30)
    GROUP BY a.id, a.policy_id, a.claim_date
),
combined_details AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        p.start_date AS policy_start_date,
        p.end_date AS policy_end_date,
        p.holder_id AS policy_holder_id,
        po.segment AS holder_segment
    FROM joined_ranges s
    JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
),
enriched AS (
    -- Join policies
    SELECT s.*, p.premium AS policy_premium, p.end_date AS policy_end_date_2
    FROM combined_details s
    JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    policy_start_date,
    policy_end_date,
    policy_holder_id,
    holder_segment,
    policy_premium
FROM enriched
ORDER BY policy_start_date, id
LIMIT 20
