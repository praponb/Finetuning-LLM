WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.status,
        s.adjuster_id,
        s.product,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.adjuster_id AS claim_adjuster_id,
        c.approved_amount AS claim_approved_amount
    FROM matched s
    LEFT JOIN claims c ON c.policy_id = s.policy_id
),
detail_rows_claims_2 AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.product,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_approved_amount,
        c.id AS claim_id_2,
        c.status AS claim_status,
        c.approved_amount AS claim_approved_amount_2,
        c.claimed_amount AS claim_claimed_amount_2,
        c.claim_date
    FROM detail_rows_claims s
    JOIN claims c ON c.policy_id = s.policy_id AND YEAR(c.claim_date) = 2024
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM detail_rows_claims_2 s
    WHERE s.adjuster_id IN (
            SELECT c.adjuster_id
            FROM claims c
        )
)
-- Final projection
SELECT
    id,
    adjuster_id,
    product,
    claim_id,
    claim_adjuster_id,
    claim_approved_amount,
    claim_id_2,
    claim_status,
    claim_approved_amount_2,
    claim_claimed_amount_2,
    claim_date
FROM with_activity
WHERE claim_date < DATE '2024-09-08'
