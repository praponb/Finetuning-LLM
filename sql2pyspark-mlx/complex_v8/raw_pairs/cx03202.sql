WITH matched AS (
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
with_details AS (
    -- Attach policy attributes
    SELECT
        s.*,
        p.start_date AS policy_start_date,
        p.premium AS policy_premium,
        p.holder_id AS policy_holder_id,
        po.joined_on AS holder_joined_on,
        po.risk_score AS holder_risk_score
    FROM matched s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(prior_rows_90d, 0) AS prior_rows_90d_filled,
        ROUND(prior_claimed_amount_90d / NULLIF(prior_rows_90d, 0), 3) AS prior_claimed_amount_90d_per_prior_rows_90d,
        ROUND(holder_risk_score * 1.5, 2) AS holder_risk_score_adj
    FROM with_details
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT SUM(c.approved_amount) FROM claims c WHERE c.policy_id = s.policy_id) AS total_claim_approved_amount,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_date
    FROM labeled s
)
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_90d,
    prior_claimed_amount_90d,
    policy_start_date,
    policy_premium,
    policy_holder_id,
    holder_joined_on,
    holder_risk_score,
    prior_rows_90d_filled,
    prior_claimed_amount_90d_per_prior_rows_90d,
    total_claim_approved_amount,
    last_claim_date
FROM profiled
ORDER BY prior_claimed_amount_90d, policy_premium DESC
