WITH covered AS (
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
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.prior_rows_90d,
        s.prior_approved_amount_90d,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claim_date AS claim_claim_date,
        c.reported_at AS claim_reported_at,
        c.claim_type
    FROM covered s
    JOIN claims c ON c.policy_id = s.policy_id
),
with_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.prior_rows_90d,
        s.prior_approved_amount_90d,
        s.claim_id,
        s.claim_status,
        s.claim_reported_at,
        s.claim_type,
        p.holder_id AS policy_holder_id,
        p.status AS policy_status
    FROM with_children s
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_details_policy s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claimed_amount < 39666.5
              AND c.status = 'IN_REVIEW'
        )
      AND s.claim_type = 'medical'
)
SELECT
    id,
    policy_id,
    prior_rows_90d,
    prior_approved_amount_90d,
    claim_id,
    claim_status,
    claim_reported_at
FROM with_activity
ORDER BY policy_id, prior_approved_amount_90d NULLS LAST
