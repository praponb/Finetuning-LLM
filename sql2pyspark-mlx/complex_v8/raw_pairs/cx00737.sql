WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_180d,
        SUM(b.claimed_amount) AS prior_claimed_amount_180d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 180)
    GROUP BY a.id, a.policy_id, a.claim_date
),
with_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_180d,
        s.prior_claimed_amount_180d,
        p.end_date AS policy_end_date,
        p.premium AS policy_premium,
        p.status AS policy_status,
        po.joined_on AS holder_joined_on,
        po.region AS holder_region
    FROM in_window s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_claimed_amount_180d,
        s.policy_premium,
        s.policy_status,
        s.holder_joined_on,
        c.id AS claim_id,
        c.claim_type,
        c.claim_date AS claim_claim_date
    FROM with_details_policy s
    JOIN claims c ON c.policy_id = s.policy_id
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_claimed_amount_180d,
        s.policy_status,
        s.claim_id,
        s.claim_claim_date,
        c.adjuster_id AS claim_adjuster_id,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount
    FROM line_level_claims s
    JOIN claims c ON c.id = s.claim_id
)
SELECT
    id,
    claim_date,
    prior_claimed_amount_180d,
    policy_status,
    claim_id,
    claim_adjuster_id,
    claim_approved_amount,
    claim_claimed_amount,
    DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter
FROM with_details
ORDER BY id, claim_claimed_amount
