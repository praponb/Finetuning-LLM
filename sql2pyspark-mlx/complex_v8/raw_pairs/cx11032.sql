WITH matched AS (
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
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id,
        RANK() OVER (ORDER BY premium) AS premium_rank
    FROM matched
),
with_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.*,
        p.start_date AS policy_start_date,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM with_trends s
    JOIN policies p ON p.id = s.policy_id AND p.end_date BETWEEN DATE '2026-01-20' AND DATE '2026-12-05'
    JOIN adjusters a ON a.id = s.adjuster_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT policy_start_date, claim_date, product, claimed_amount
    FROM with_details_policy
) src
PIVOT (
    AVG(claimed_amount) AS avg_v, SUM(claimed_amount) AS sum_v
    FOR product IN ('AUTO', 'TRAVEL')
)
ORDER BY policy_start_date, claim_date
