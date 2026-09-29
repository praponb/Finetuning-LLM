WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_90d,
        SUM(b.claimed_amount) AS prior_claimed_amount_90d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 90)
    GROUP BY a.id, a.policy_id, a.claim_date
),
combined_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_90d,
        s.prior_claimed_amount_90d,
        p.holder_id AS policy_holder_id,
        p.end_date AS policy_end_date,
        p.premium AS policy_premium,
        po.joined_on AS holder_joined_on
    FROM joined_ranges s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
labeled AS (
    -- Derive helper columns
    SELECT *, YEAR(policy_end_date) AS policy_end_date_year
    FROM combined_details_policy
    WHERE policy_id IS NOT NULL
)
SELECT
    policy_id,
    policy_end_date_year,
    GROUPING_ID() AS grouping_level,
    MIN(claim_date) AS first_claim_date,
    COUNT(DISTINCT policy_id) AS distinct_policy_ids
FROM labeled
GROUP BY CUBE(policy_id, policy_end_date_year)
ORDER BY policy_id NULLS LAST, policy_end_date_year NULLS LAST
