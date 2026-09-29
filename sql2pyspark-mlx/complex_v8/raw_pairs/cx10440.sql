WITH cleaned_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        QUARTER(claim_date) AS claim_date_quarter,
        TO_DATE(reported_at) AS reported_at_date
    FROM claims
    WHERE policy_id <= 29
      AND claim_type IS NOT NULL
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.claim_date_quarter,
        s.reported_at_date,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount,
        c.status AS claim_status
    FROM cleaned_claims s
    JOIN claims c ON c.policy_id = s.policy_id
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date_quarter,
        s.reported_at_date,
        s.claim_id,
        s.claim_approved_amount,
        s.claim_status,
        c.id AS claim_id_2,
        c.claimed_amount AS claim_claimed_amount_2,
        c.approved_amount AS claim_approved_amount_2
    FROM detail_rows s
    LEFT JOIN claims c ON c.policy_id = s.policy_id
)
SELECT *
FROM (
    SELECT claim_id_2, claim_status, claim_approved_amount_2
    FROM line_level_claims
) src
PIVOT (
    SUM(claim_approved_amount_2) AS amount, COUNT(claim_approved_amount_2) AS n
    FOR claim_status IN ('IN_REVIEW', 'OPEN', 'REJECTED', 'APPROVED')
)
ORDER BY claim_id_2
