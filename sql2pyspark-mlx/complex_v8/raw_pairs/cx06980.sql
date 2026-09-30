WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        policy_id,
        claim_type,
        SUM(CASE WHEN claim_type <> 'collision' THEN 1 ELSE 0 END) AS n_claim_type_collision,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids
    FROM covered
    GROUP BY DATE_TRUNC('MONTH', claim_date), policy_id, claim_type
    HAVING AVG(claimed_amount) > 17585.2
),
with_children AS (
    -- One row per claim
    SELECT
        s.claim_date_month,
        s.policy_id,
        s.claim_type,
        s.n_claim_type_collision,
        s.distinct_policy_ids,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at
    FROM per_group s
    LEFT JOIN claims c ON c.policy_id = s.policy_id AND c.adjuster_id IS NOT NULL
),
with_parent_adjuster AS (
    -- Join adjusters
    SELECT
        s.claim_date_month,
        s.policy_id,
        s.claim_type,
        s.n_claim_type_collision,
        s.distinct_policy_ids,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_approved_amount,
        s.claim_reported_at,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM with_children s
    JOIN adjusters a ON a.id = s.claim_adjuster_id
)
-- Final projection
SELECT
    claim_date_month,
    policy_id,
    claim_type,
    n_claim_type_collision,
    distinct_policy_ids,
    claim_id,
    claim_approved_amount,
    claim_reported_at,
    adjuster_team,
    adjuster_level,
    adjuster_name
FROM with_parent_adjuster
ORDER BY claim_type DESC, claim_date_month DESC, policy_id, claim_id
LIMIT 10
