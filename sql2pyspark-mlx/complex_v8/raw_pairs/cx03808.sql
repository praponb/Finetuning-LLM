WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_180d,
        SUM(b.approved_amount) AS prior_approved_amount_180d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 180)
    GROUP BY a.id, a.policy_id, a.claim_date
),
joined_policy AS (
    -- Enrich with dimension data
    SELECT s.*, p.status AS policy_status, p.premium AS policy_premium
    FROM in_window s
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.prior_approved_amount_180d,
        s.policy_status,
        s.policy_premium,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claim_date AS claim_claim_date,
        c.claim_type
    FROM joined_policy s
    JOIN claims c ON c.policy_id = s.policy_id
)
SELECT
    id,
    prior_approved_amount_180d,
    policy_premium,
    claim_id,
    claim_approved_amount,
    claim_claim_date,
    claim_type,
    CASE WHEN claim_type IN ('medical', 'fire') THEN 'group_a' WHEN claim_type = 'theft' THEN 'group_b' ELSE 'other' END AS claim_type_group
FROM with_children_claims
WHERE claim_approved_amount > 2272.7
ORDER BY id NULLS LAST, claim_claim_date
