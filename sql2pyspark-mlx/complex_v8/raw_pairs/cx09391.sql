WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_30d,
        SUM(b.approved_amount) AS prior_approved_amount_30d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 30)
    GROUP BY a.id, a.policy_id, a.claim_date
),
with_parent_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_30d,
        s.prior_approved_amount_30d,
        p.end_date AS policy_end_date
    FROM covered s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.end_date < DATE '2026-09-23'
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) AS claim_count,
        MAX(reported_at) AS last_claim_reported_at
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.last_claim_reported_at
    FROM with_parent_policy s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_30d,
        s.policy_end_date,
        s.claim_count,
        s.last_claim_reported_at,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id
    FROM with_stats s
    JOIN claims c ON c.policy_id = s.policy_id AND c.status <> 'OPEN'
)
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_30d,
    policy_end_date,
    claim_count,
    last_claim_reported_at,
    claim_id,
    claim_claimed_amount,
    claim_status,
    claim_adjuster_id
FROM line_level_claims
